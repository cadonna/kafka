/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class MetricsAggregations implements MetricsReporter {

    private static class MetricToAggregateSpec {
        public final String name;
        public final String group;

        public MetricToAggregateSpec(final String name, final String group) {
            this.name = name;
            this.group = group;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof MetricToAggregateSpec)) return false;
            final MetricToAggregateSpec that = (MetricToAggregateSpec) o;
            return name.equals(that.name) &&
                group.equals(that.group);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, group);
        }
    }

    private static class AggregationSpec<AGG, V> {
        public final String name;
        public final Collection<String> tagsForGrouping;
        public final MetricRegistrar<AGG, V> metricRegistrar;

        public AggregationSpec(final String name,
                               final Collection<String> tagsForGrouping,
                               final MetricRegistrar<AGG, V> metricRegistrar) {
            this.name = name;
            this.tagsForGrouping = tagsForGrouping;
            this.metricRegistrar = metricRegistrar;
        }
    }

    @SuppressWarnings("unchecked")
    public static class ValuesProvider<V> implements Iterable<V> {

        private class ValueIterator implements Iterator<V> {

            private final Iterator<Metric> iterator;

            public ValueIterator(final Collection<Metric> metrics) {
                this.iterator = metrics.iterator();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public V next() {
                return (V) iterator.next().metricValue();
            }
        }

        private final Map<MetricName, Metric> metricsToAggregate = new ConcurrentHashMap<>();

        public Iterator<V> iterator() {
            return new ValueIterator(Collections.unmodifiableCollection(metricsToAggregate.values()));
        }

        public void addMetric(final MetricName metricName, final Metric metric) {
            metricsToAggregate.put(metricName, metric);
        }

        public void removeMetric(final MetricName metricName) {
            metricsToAggregate.remove(metricName);
        }

        public boolean isEmpty() {
            return metricsToAggregate.isEmpty();
        }
    }

    public interface MetricRegistrar<AGG, V> {
        ValuesProvider<V> register(final Map<String, String> tags);
        void deregister();
    }

    private interface Updater {
        void update(final KafkaMetric metric,
                    final AggregationSpec<?, ?> aggregationSpec,
                    final Map<String, String> tags,
                    final MetricName metricNameForAggregation);
    }

    private final Map<MetricToAggregateSpec, List<AggregationSpec<?, ?>>> metricSpecsToAggregationSpecs = new HashMap<>();
    private final Map<MetricName, Map<String, ValuesProvider<?>>> metricsToValuesProviders = new HashMap<>();

    @Override
    public void init(final List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    public <AGG, V> void addAggregation(final String nameOfAggregation,
                                        final String groupOfMetricsToAggregate,
                                        final String nameOfMetricsToAggregate,
                                        final List<String> tagsForGrouping,
                                        final MetricRegistrar<AGG, V> metricRegistrar) {
        metricSpecsToAggregationSpecs.computeIfAbsent(
            new MetricToAggregateSpec(nameOfMetricsToAggregate, groupOfMetricsToAggregate), (ignored) -> new LinkedList<>()
        ).add(new AggregationSpec<>(nameOfAggregation, tagsForGrouping, metricRegistrar));
    }

    private void updateAggregationMetrics(final KafkaMetric metric,
                                          final Updater updater) {
        if (!metricSpecsToAggregationSpecs.isEmpty()) {
            final MetricToAggregateSpec metricToAggregateSpec = new MetricToAggregateSpec(
                metric.metricName().name(),
                metric.metricName().group()
            );
            if (metricSpecsToAggregationSpecs.containsKey(metricToAggregateSpec)) {
                final Map<String, String> metricTagMap = metric.metricName().tags();
                for (final AggregationSpec<?, ?> aggregationSpec : metricSpecsToAggregationSpecs.get(metricToAggregateSpec)) {
                    final Map<String, String> tags = new HashMap<>();
                    for (final String tagForGrouping : aggregationSpec.tagsForGrouping) {
                        tags.put(tagForGrouping, metricTagMap.getOrDefault(tagForGrouping, "unknown"));
                    }
                    final MetricName metricNameForAggregation = new MetricName(
                        metric.metricName().name(),
                        metric.metricName().group(),
                        "",
                        tags
                    );
                    updater.update(metric, aggregationSpec, tags, metricNameForAggregation);
                }
            }
        }
    }

    @Override
    public void metricChange(final KafkaMetric metric) {
        updateAggregationMetrics(metric, this::addMetric);
    }

    private void addMetric(final KafkaMetric metric, final AggregationSpec<?, ?> aggregationSpec, final Map<String, String> tags, final MetricName metricNameForAggregation) {
        metricsToValuesProviders
            .computeIfAbsent(metricNameForAggregation, (ignored) -> new HashMap<>())
            .computeIfAbsent(
                aggregationSpec.name,
                (ignored) -> aggregationSpec.metricRegistrar.register(Collections.unmodifiableMap(tags))
            ).addMetric(metric.metricName(), metric);
    }

    @Override
    public void metricRemoval(final KafkaMetric metric) {
        updateAggregationMetrics(metric, this::removeMetric);
    }

    private void removeMetric(final KafkaMetric metric, final AggregationSpec<?, ?> aggregationSpec, final Map<String, String> tags, final MetricName metricNameForAggregation) {
        final Map<String, ValuesProvider<?>> aggregationNamesToValuesProvider = metricsToValuesProviders
            .computeIfAbsent(
                metricNameForAggregation,
                (ignored) -> {throw new IllegalStateException("no aggregation metric found");}
            );
        final ValuesProvider<?> valuesProvider = aggregationNamesToValuesProvider.computeIfAbsent(
            aggregationSpec.name,
            (ignored) -> {throw new IllegalStateException("no values provider found");}
        );
        valuesProvider.removeMetric(metric.metricName());
        if (valuesProvider.isEmpty()) {
            aggregationSpec.metricRegistrar.deregister();
            aggregationNamesToValuesProvider.remove(aggregationSpec.name);
            if (aggregationNamesToValuesProvider.isEmpty()) {
                metricsToValuesProviders.remove(metricNameForAggregation);
            }
        }
    }
}