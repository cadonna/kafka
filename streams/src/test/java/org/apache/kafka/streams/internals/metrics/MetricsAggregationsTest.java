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
package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.MetricsAggregations;
import org.apache.kafka.streams.MetricsAggregations.MetricRegistrar;
import org.apache.kafka.streams.MetricsAggregations.ValuesProvider;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;


public class MetricsAggregationsTest {

    private static final String AGGREGATION_NAME1 = "test-aggregation1";
    private static final String TAG1 = "tag1";
    private static final String TAG2 = "tag2";
    private static final String TAG3 = "tag3";
    private static final String VALUE1 = "value1";
    private static final String VALUE2 = "value2";
    private static final String VALUE3 = "value3";
    private static final String VALUE4 = "value4";
    private static final Map<String, String> TAGS1 = mkMap(
        mkEntry(TAG1, VALUE1),
        mkEntry(TAG2, VALUE2),
        mkEntry(TAG3, VALUE3)
    );
    private static final Map<String, String> TAGS2 = mkMap(
        mkEntry(TAG1, VALUE1),
        mkEntry(TAG2, VALUE2),
        mkEntry(TAG3, VALUE4)
    );
    private static final MetricName METRIC_NAME1 = new MetricName(
        "name1",
        "group1",
        "test-description",
        TAGS1
    );
    private static final MetricName METRIC_NAME2 = new MetricName(
        "name1",
        "group1",
        "test-description",
        TAGS2
    );
    private static final MetricName METRIC_NAME3 = new MetricName(
        "name2",
        "group2",
        "test-description",
        TAGS1
    );
    private static final Gauge<Integer> VALUE_PROVIDER = (now, config) -> 1;

    private final MetricRegistrar<Long, Integer> metricRegistrar1 = mock(MetricRegistrar.class);
    private final MetricRegistrar<Long, Integer> metricRegistrar2 = mock(MetricRegistrar.class);
    private final MetricRegistrar<Long, Integer> metricRegistrar3 = mock(MetricRegistrar.class);

    public static class MockMetricsAggregations extends MetricsAggregations {

        @Override
        public void close() {
        }

        @Override
        public void configure(final Map<String, ?> configs) {
        }
    }
    final MockMetricsAggregations mockMetricsAggregations = new MockMetricsAggregations();

    @Test
    public void shouldNotRegisterOrAddAnythingIfNoAggregationAdded() {
        shouldNotDoAnythingIfNoAggregationAdded(mockMetricsAggregations::metricChange);
    }

    @Test
    public void shouldNotDeregisterOrRemoveAnythingIfNoAggregationAdded() {
        shouldNotDoAnythingIfNoAggregationAdded(mockMetricsAggregations::metricRemoval);
    }

    private void shouldNotDoAnythingIfNoAggregationAdded(final Consumer<KafkaMetric> metricUpdate) {
        final KafkaMetric changedMetric = getMetric(METRIC_NAME1);
        replay(metricRegistrar1);

        metricUpdate.accept(changedMetric);

        verify(metricRegistrar1);
    }

    @Test
    public void shouldNotRegisterOrAddAnythingIfNoAggregationsAddedForChangedMetric() {
        shouldNotDoAnythingIfNoAggregationsAddedForUpdatedMetric(mockMetricsAggregations::metricChange);
    }

    @Test
    public void shouldNotDeregisterOrRemoveAnythingIfNoAggregationsAddedForRemovedMetric() {
        shouldNotDoAnythingIfNoAggregationsAddedForUpdatedMetric(mockMetricsAggregations::metricRemoval);
    }

    private void shouldNotDoAnythingIfNoAggregationsAddedForUpdatedMetric(final Consumer<KafkaMetric> metricUpdate) {
        mockMetricsAggregations.addAggregation(
            "aggregation for metrics with different group and name as updated metric",
            METRIC_NAME3.group(),
            METRIC_NAME3.name(),
            Collections.emptyList(),
            metricRegistrar1
        );
        mockMetricsAggregations.addAggregation(
            "aggregation for metrics with different name but equal group as updated metric",
            METRIC_NAME1.group(),
            METRIC_NAME3.name(),
            Collections.emptyList(),
            metricRegistrar1
        );
        mockMetricsAggregations.addAggregation(
            "aggregation for metrics with different group but equal name as updated metric",
            METRIC_NAME3.group(),
            METRIC_NAME1.name(),
            Collections.emptyList(),
            metricRegistrar1
        );
        final KafkaMetric changedMetric = getMetric(METRIC_NAME1);
        replay(metricRegistrar1);

        metricUpdate.accept(changedMetric);

        verify(metricRegistrar1);
    }

    @Test
    public void shouldRegisterAggregationMetrics() {
        mockMetricsAggregations.addAggregation(
            "first aggregation for updated metric",
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Arrays.asList(TAG1, TAG2),
            metricRegistrar1
        );
        mockMetricsAggregations.addAggregation(
            "second aggregation for updated metric",
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Collections.singletonList(TAG2),
            metricRegistrar2
        );
        mockMetricsAggregations.addAggregation(
            "aggregation not for updated metrics",
            METRIC_NAME3.group(),
            METRIC_NAME3.name(),
            Collections.singletonList(TAG2),
            metricRegistrar3
        );
        final KafkaMetric changedMetric = getMetric(METRIC_NAME1);
        final Map<String, String> groupTags1 = mkMap(mkEntry(TAG1, TAGS1.get(TAG1)), mkEntry(TAG2, TAGS1.get(TAG2)));
        final Map<String, String> groupTags2 = mkMap(mkEntry(TAG2, TAGS1.get(TAG2)));
        expect(metricRegistrar1.register(groupTags1)).andReturn(new ValuesProvider<>());
        expect(metricRegistrar2.register(groupTags2)).andReturn(new ValuesProvider<>());
        replay(metricRegistrar1, metricRegistrar2, metricRegistrar3);

        mockMetricsAggregations.metricChange(changedMetric);

        verify(metricRegistrar1, metricRegistrar2, metricRegistrar3);
    }

    @Test
    public void shouldRegisterAggregationMetricOnce() {
        mockMetricsAggregations.addAggregation(
            AGGREGATION_NAME1,
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Arrays.asList(TAG1, TAG2),
            metricRegistrar1
        );
        final KafkaMetric changedMetric1 = getMetric(METRIC_NAME1);
        final KafkaMetric changedMetric2 = getMetric(METRIC_NAME2);
        final Map<String, String> groupTags1 = mkMap(mkEntry(TAG1, TAGS1.get(TAG1)), mkEntry(TAG2, TAGS1.get(TAG2)));
        expect(metricRegistrar1.register(groupTags1)).andReturn(new ValuesProvider<>());
        replay(metricRegistrar1);

        mockMetricsAggregations.metricChange(changedMetric1);
        mockMetricsAggregations.metricChange(changedMetric2);

        verify(metricRegistrar1);
    }

    @Test
    public void shouldDeregisterAggregationMetrics() {
        mockMetricsAggregations.addAggregation(
            "first aggregation for updated metric",
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Arrays.asList(TAG1, TAG2),
            metricRegistrar1
        );
        mockMetricsAggregations.addAggregation(
            "second aggregation for updated metric",
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Arrays.asList(TAG1, TAG2, TAG3),
            metricRegistrar2
        );
        mockMetricsAggregations.addAggregation(
            "aggregation not for updated metrics",
            METRIC_NAME3.group(),
            METRIC_NAME3.name(),
            Collections.singletonList(TAG2),
            metricRegistrar3
        );
        final KafkaMetric changedMetric = getMetric(METRIC_NAME1);
        expect(metricRegistrar1.register(anyObject())).andReturn(new ValuesProvider<>());
        expect(metricRegistrar2.register(anyObject())).andReturn(new ValuesProvider<>());
        metricRegistrar1.deregister();
        metricRegistrar2.deregister();
        replay(metricRegistrar1, metricRegistrar2, metricRegistrar3);
        mockMetricsAggregations.metricChange(changedMetric);

        mockMetricsAggregations.metricRemoval(changedMetric);

        verify(metricRegistrar1, metricRegistrar2, metricRegistrar3);
    }

    @Test
    public void shouldDeregisterAggregationMetricOnlyWhenLastMetricIsRemovedFromAggregation() {
        mockMetricsAggregations.addAggregation(
            AGGREGATION_NAME1,
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Arrays.asList(TAG1, TAG2),
            metricRegistrar1
        );
        final KafkaMetric changedMetric1 = getMetric(METRIC_NAME1);
        final KafkaMetric changedMetric2 = getMetric(METRIC_NAME2);
        expect(metricRegistrar1.register(anyObject())).andReturn(new ValuesProvider<>());
        replay(metricRegistrar1);
        mockMetricsAggregations.metricChange(changedMetric1);
        mockMetricsAggregations.metricChange(changedMetric2);

        mockMetricsAggregations.metricRemoval(changedMetric2);
        verify(metricRegistrar1);

        reset(metricRegistrar1);
        metricRegistrar1.deregister();
        replay(metricRegistrar1);
        mockMetricsAggregations.metricRemoval(changedMetric1);
        verify(metricRegistrar1);
    }

    @Test
    public void shouldRegisterAggregationMetricsWithUnknownTag() {
        final String unknownTag = "unknown-tag";
        mockMetricsAggregations.addAggregation(
            AGGREGATION_NAME1,
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Collections.singletonList(unknownTag),
            metricRegistrar1
        );
        final KafkaMetric changedMetric = getMetric(METRIC_NAME1);
        final Map<String, String> groupTags = mkMap(mkEntry("unknown-tag", "unknown"));
        expect(metricRegistrar1.register(groupTags)).andReturn(new ValuesProvider<>());
        replay(metricRegistrar1);

        mockMetricsAggregations.metricChange(changedMetric);

        verify(metricRegistrar1);
    }

    @Test
    public void shouldAddMetricsToValuesProviderForAggregation() {
        shouldAddMultipleMetricsToValuesProviderForAggregation(
            (metrics) -> metrics.forEach(mockMetricsAggregations::metricChange));
    }

    @Test
    public void shouldAddMetricsToValuesProviderForAggregationDuringInit() {
        shouldAddMultipleMetricsToValuesProviderForAggregation(mockMetricsAggregations::init);
    }

    private void shouldAddMultipleMetricsToValuesProviderForAggregation(final Consumer<List<KafkaMetric>> metricsChanger) {
        final List<Metric> metrics = new ArrayList<>();
        mockMetricsAggregations.addAggregation(
            AGGREGATION_NAME1,
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Collections.emptyList(),
            new MetricRegistrar<Integer, Integer>() {
                @Override
                public ValuesProvider<Integer> register(final Map<String, String> tags) {
                    final ValuesProvider<Integer> valuesProvider = new ValuesProvider<>();
                    metrics.add(new KafkaMetric(
                        new Object(),
                        new MetricName(AGGREGATION_NAME1, "", "", Collections.emptyMap()),
                        (Gauge<Integer>) (config, now) -> {
                            int aggregate = 0;
                            for (final int value : valuesProvider) {
                                aggregate = aggregate + value;
                            }
                            return aggregate;
                        },
                        new MetricConfig(),
                        new MockTime()
                    ));
                    return valuesProvider;
                }

                @Override
                public void deregister() {
                }
            }
        );
        final KafkaMetric changedMetric1 = getMetric(METRIC_NAME1);
        final KafkaMetric changedMetric2 = getMetric(METRIC_NAME2);
        final KafkaMetric changedMetric3 = getMetric(METRIC_NAME3);

        metricsChanger.accept(Arrays.asList(changedMetric1, changedMetric2, changedMetric3));

        assertThat(metrics.get(0).metricValue(), is(2));
    }

    @Test
    public void shouldThrowIfAMetricIsRemovedForWhichNoAgggregationGroupExists() {
        mockMetricsAggregations.addAggregation(
            "first aggregation for updated metric",
            METRIC_NAME1.group(),
            METRIC_NAME1.name(),
            Arrays.asList(TAG1, TAG2),
            metricRegistrar1
        );
        final KafkaMetric changedMetric = getMetric(METRIC_NAME1);

        assertThrows(IllegalStateException.class, () -> mockMetricsAggregations.metricRemoval(changedMetric));
    }

    private KafkaMetric getMetric(final MetricName metricName) {
        return new KafkaMetric(new Object(), metricName, VALUE_PROVIDER, new MetricConfig(), new MockTime());
    }
}