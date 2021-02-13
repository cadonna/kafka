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

/**
 * A metrics reporter that adds and removes metrics to aggregations.
 * <p>
 * After users add specifications of aggregations to this reporter, every time a metric is added or modified, this
 * reporter adds the metric to the aggregations the metric contributes to.
 * Every time a metric is removed, the reporter removes the metric from the aggregations is was added to previously.
 * <p>
 * The first metric that is added to an aggregation triggers a registration callback that users can use to register
 * the metric that records the aggregation to their own metric framework.
 * Similarly, removal of the last metric of an aggregation triggers a deregistration callback that users can use to
 * deregisters the metric that records the aggregation from the their metric framework.
 * <p>
 * A registration callback returns a values provider which allows to iterate over all values of the metrics that
 * contribute to the aggregation.
 * This values provider can be used to compute the actual aggregation that is recorded by the metric that reports the
 * aggregation.
 * <p>
 * The added aggregations may specify a list of tags according to which the metrics to aggregate are grouped for
 * aggregation.
 * <p>
 * This reporter needs to be extended and the child class can be added to the {@link StreamsConfig#METRIC_REPORTER_CLASSES_CONFIG}.
 * The reporter is automatically instantiated by the {@link KafkaStreams} object.
 */
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
                    final MetricName metricNameForAggregationGroup);
    }

    private final Map<MetricToAggregateSpec, List<AggregationSpec<?, ?>>> metricSpecsToAggregationSpecs = new HashMap<>();
    private final Map<MetricName, Map<String, ValuesProvider<?>>> metricsAggregationGroupsToValuesProviders = new HashMap<>();

    /**
     * Adds all currently existing metrics to their aggregations if any exist.
     *
     * @param metrics All currently existing metrics
     */
    @Override
    public void init(final List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    /**
     * Adds an aggregation to the reporter.
     *
     * An aggregation has a name. The metrics that contribute to the aggregate are specified by their name and the group
     * they belong to.
     * For example, an aggregation that sums up the memtable sizes of the RocksDB state stores aggregates metrics
     * with name "size-all-mem-tables" and group "stream-state-metrics".
     * An aggregation can also specify a list of tags by which to group metrics for the aggregation.
     * For example, to aggregate metrics grouped by stream thread ID and task ID, the list of tags needs to contain
     * "task-id" and "thread-id".
     * Each aggregation has a registration and deregistration callback that is used to add and remove the metric that
     * records the aggregation from the users' metric framework.
     * The registration callback returns a {@link ValuesProvider} that can be used to iterate over all metrics that
     * are added to this aggregation.
     *
     * <p>
     * An example for the specification of an aggregation (calls to the metrics framework are fictive):
     * <pre>{@code
     * String nameOfAggregation = "size-all-mem-tables-total";
     * metricsAggregations.addAggregation(
     *     nameOfAggregation,
     *     "stream-state-metrics",
     *     "size-all-mem-tables",
     *     Arrays.asList("thread-id"),
     *     new MetricRegistrar<BigInteger, BigInteger>() {
     *         @Override
     *         public ValuesProvider<Integer> register(final Map<String, String> tags) {
     *             final ValuesProvider<Integer> valuesProvider = new ValuesProvider<>();
     *             metricsRegistry.register(
     *                 "nameOfAggregation",
     *                 (now) -> {
     *                     BigInteger aggregate = 0;
     *                     for (final int value : valuesProvider) {
     *                         aggregate = aggregate.add(value);
     *                     }
     *                     return aggregate;
     *                 }
     *             );
     *             return valuesProvider;
     *         }
     *
     *         @Override
     *         public void deregister() {
     *             metricsRegistry.deregister(nameOfAggregation);
     *         }
     *     }
     * );
     * }</pre>
     * This aggregation sums up the sizes of the memtables in RocksDB state stores that exist in the Kafka Streams
     * client grouped by stream thread ID.
     *
     * @param nameOfAggregation          name of the aggregation
     * @param groupOfMetricsToAggregate  group of the metrics to aggregate
     * @param nameOfMetricsToAggregate   name of the metrics to aggregate
     * @param tagsForGrouping            tags for grouping
     * @param metricRegistrar            (de-)registration callback
     * @param <AGG>                      type of the aggregation
     * @param <V>                        type of the values recorded by the metrics that contribute to the aggregation
     */
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
                    final MetricName metricNameForAggregationGroup = new MetricName(
                        metric.metricName().name(),
                        metric.metricName().group(),
                        "",
                        tags
                    );
                    updater.update(metric, aggregationSpec, tags, metricNameForAggregationGroup);
                }
            }
        }
    }

    /**
     * Adds a metric to the aggregations it contributes to if any exist.
     */
    @Override
    public void metricChange(final KafkaMetric metric) {
        updateAggregationMetrics(metric, this::addMetric);
    }

    private synchronized void addMetric(final KafkaMetric metric,
                                        final AggregationSpec<?, ?> aggregationSpec,
                                        final Map<String, String> tags,
                                        final MetricName metricNameForAggregationGroup) {
        metricsAggregationGroupsToValuesProviders
            .computeIfAbsent(metricNameForAggregationGroup, (ignored) -> new HashMap<>())
            .computeIfAbsent(
                aggregationSpec.name,
                (ignored) -> aggregationSpec.metricRegistrar.register(Collections.unmodifiableMap(tags))
            ).addMetric(metric.metricName(), metric);
    }

    /**
     * Removes a metric from the aggregations it contributes to if any exist.
     */
    @Override
    public void metricRemoval(final KafkaMetric metric) {
        updateAggregationMetrics(metric, this::removeMetric);
    }

    private synchronized void removeMetric(final KafkaMetric metric,
                                           final AggregationSpec<?, ?> aggregationSpec,
                                           final Map<String, String> tags,
                                           final MetricName metricNameForAggregationGroup) {
        final Map<String, ValuesProvider<?>> aggregationNamesToValuesProvider = metricsAggregationGroupsToValuesProviders
            .computeIfAbsent(
                metricNameForAggregationGroup,
                (ignored) -> {
                    throw new IllegalStateException("No aggregation group found for metric name "
                        + metricNameForAggregationGroup + ". This is a bug in Kafka Streams." +
                        "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
                }
            );
        final ValuesProvider<?> valuesProvider = aggregationNamesToValuesProvider.computeIfAbsent(
            aggregationSpec.name,
            (ignored) -> {
                throw new IllegalStateException("No values provider found for aggregation "
                    + aggregationSpec.name + ". This is a bug in Kafka Streams." +
                    "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
            }
        );
        valuesProvider.removeMetric(metric.metricName());
        if (valuesProvider.isEmpty()) {
            aggregationSpec.metricRegistrar.deregister();
            aggregationNamesToValuesProvider.remove(aggregationSpec.name);
            if (aggregationNamesToValuesProvider.isEmpty()) {
                metricsAggregationGroupsToValuesProviders.remove(metricNameForAggregationGroup);
            }
        }
    }
}