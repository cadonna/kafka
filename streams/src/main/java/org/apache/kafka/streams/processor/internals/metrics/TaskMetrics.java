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
package org.apache.kafka.streams.processor.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;

public class TaskMetrics {
    private TaskMetrics() {}

    private static final String COMMIT = "commit";
    private static final String COMMIT_LATENCY = COMMIT + LATENCY_SUFFIX;
    private static final String PROCESS = "process";
    private static final String PROCESS_LATENCY = PROCESS + LATENCY_SUFFIX;
    private static final String PUNCTUATE = "punctuate";
    private static final String PUNCTUATE_LATENCY = PUNCTUATE + LATENCY_SUFFIX;
    private static final String ENFORCED_PROCESSING = "enforced-processing";
    private static final String RECORD_LATENESS = "record-lateness";
    private static final String DROPPED_RECORDS = "dropped-records";

    private static final String TOTAL_DESCRIPTION = "The total number of ";
    private static final String RATE_DESCRIPTION = "The average per-second number of ";
    private static final String COMMIT_DESCRIPTION = "commit calls";
    private static final String COMMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;
    private static final String COMMIT_RATE_DESCRIPTION = RATE_DESCRIPTION + COMMIT_DESCRIPTION;
    private static final String PUNCTUATE_DESCRIPTION = "punctuate calls";
    private static final String PUNCTUATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_RATE_DESCRIPTION = RATE_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String ENFORCED_PROCESSING_TOTAL_DESCRIPTION =
        "The total number of occurrence of enforced-processing operations";
    private static final String ENFORCED_PROCESSING_RATE_DESCRIPTION =
        "The average number of occurrence of enforced-processing operation per second";
    private static final String RECORD_LATENESS_MAX_DESCRIPTION = "The max observed lateness of records";
    private static final String RECORD_LATENESS_AVG_DESCRIPTION = "The average observed lateness of records";
    private static final String DROPPED_RECORDS_DESCRIPTION = "dropped records";
    private static final String DROPPED_RECORDS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + DROPPED_RECORDS_DESCRIPTION;
    private static final String DROPPED_RECORDS_RATE_DESCRIPTION = RATE_DESCRIPTION + DROPPED_RECORDS_DESCRIPTION;
    private static final String PROCESS_DESCRIPTION = "process calls";
    private static final String PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATE_DESCRIPTION = RATE_DESCRIPTION + PROCESS_DESCRIPTION;


    public static Sensor processLatencySensor(final String taskId,
                                              final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.LATEST) {
            final Sensor sensor = streamsMetrics.taskLevelSensor(taskId, PROCESS_LATENCY, RecordingLevel.DEBUG);
            addAvgAndMaxToSensor(sensor, TASK_LEVEL_GROUP, streamsMetrics.taskLevelTagMap(taskId), PROCESS_LATENCY);
            return sensor;
        }
        return null;
    }

    public static Sensor punctuateSensor(final String taskId,
                                         final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.LATEST) {
            final Sensor sensor = streamsMetrics.taskLevelSensor(taskId, PUNCTUATE, RecordingLevel.DEBUG);
            final Map<String, String> tagMap = streamsMetrics.taskLevelTagMap(taskId);
            addAvgAndMaxToSensor(sensor, TASK_LEVEL_GROUP, tagMap, PUNCTUATE_LATENCY);
            addInvocationRateAndCountToSensor(
                sensor,
                TASK_LEVEL_GROUP,
                tagMap,
                PUNCTUATE,
                PUNCTUATE_TOTAL_DESCRIPTION,
                PUNCTUATE_RATE_DESCRIPTION
            );
            return sensor;
        }
        return null;
    }

    public static Sensor commitSensor(final String taskId,
                                      final StreamsMetricsImpl streamsMetrics,
                                      final Sensor... parents) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(taskId, COMMIT, Sensor.RecordingLevel.DEBUG, parents);
        final Map<String, String> tagMap = streamsMetrics.taskLevelTagMap(taskId);
        addAvgAndMaxToSensor(sensor, TASK_LEVEL_GROUP, tagMap, COMMIT_LATENCY);
        addInvocationRateAndCountToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            tagMap,
            COMMIT,
            COMMIT_TOTAL_DESCRIPTION,
            COMMIT_RATE_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor enforcedProcessingSensor(final String taskId,
                                                  final StreamsMetricsImpl streamsMetrics,
                                                  final Sensor... parents) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(taskId, ENFORCED_PROCESSING, RecordingLevel.DEBUG, parents);
        addInvocationRateAndCountToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(taskId),
            ENFORCED_PROCESSING,
            ENFORCED_PROCESSING_TOTAL_DESCRIPTION,
            ENFORCED_PROCESSING_RATE_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor recordLatenessSensor(final String taskId,
                                              final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(taskId, RECORD_LATENESS, RecordingLevel.DEBUG);
        addAvgAndMaxToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(taskId),
            RECORD_LATENESS,
            RECORD_LATENESS_AVG_DESCRIPTION,
            RECORD_LATENESS_MAX_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor droppedRecordsSensor(final String taskId,
                                              final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(taskId, DROPPED_RECORDS, RecordingLevel.INFO);
        addInvocationRateAndCountToSensor(
            sensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(taskId),
            DROPPED_RECORDS,
            DROPPED_RECORDS_TOTAL_DESCRIPTION,
            DROPPED_RECORDS_RATE_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor droppedRecordsSensorOrSkippedRecordsSensor(final String taskId,
                                                                    final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.FROM_100_TO_23) {
            return ThreadMetrics.skipRecordSensor(streamsMetrics);
        }
        return droppedRecordsSensor(taskId, streamsMetrics);
    }
}
