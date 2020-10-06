package org.apache.kafka.streams.internals.metrics;

import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StreamsMetricsAggregations extends MetricsAggregations {

    private final String clientId;
    private final StreamsMetricsImpl streamsMetrics;

    public StreamsMetricsAggregations(final StreamsMetricsImpl streamsMetrics, final String clientId) {
        this.streamsMetrics = streamsMetrics;
        this.clientId = clientId;
    }

    public <AGG, V> void addAggregation(final String nameOfAggregation,
                                        final String description,
                                        final RecordingLevel recordingLevel,
                                        final String groupOfMetricsToAggregate,
                                        final String nameOfMetricsToAggregate,
                                        final List<String> tagsForGrouping,
                                        final Function<ValuesProvider<V>, AGG> aggregator) {
        super.addAggregation(
            nameOfAggregation,
            groupOfMetricsToAggregate,
            nameOfMetricsToAggregate,
            tagsForGrouping,
            new MetricsRegistrar<AGG, V>() {
                @Override
                public ValuesProvider<V> register(final Map<String, String> tags) {
                    final ValuesProvider<V> valuesProvider = new ValuesProvider<>();
                    final Map<String, String> tagsForRegistration = new HashMap<>(tags);
                    tagsForRegistration.put("client-id", clientId);
                    streamsMetrics.addClientLevelMutableMetric(
                        nameOfAggregation,
                        description,
                        tagsForRegistration,
                        recordingLevel,
                        (metricConfig, now) -> aggregator.apply(valuesProvider)
                    );
                    return valuesProvider;
                }

                @Override
                public void deregister() {
                }
            }
        );
    }

    @Override
    public void configure(final Map<String, ?> configs) {

    }

    @Override
    public void close() {
    }
}
