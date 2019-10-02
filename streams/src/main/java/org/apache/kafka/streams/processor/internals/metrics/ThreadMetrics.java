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
import java.util.Optional;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP_0100_TO_23;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;

public class ThreadMetrics {
    private ThreadMetrics() {}

    private static final String COMMIT = "commit";
    private static final String POLL = "poll";
    private static final String PROCESS = "process";
    private static final String PUNCTUATE = "punctuate";
    private static final String CREATE_TASK = "task-created";
    private static final String CLOSE_TASK = "task-closed";
    private static final String SKIP_RECORD = "skipped-records";

    private static final String TOTAL_DESCRIPTION = "The total number of ";
    private static final String RATE_DESCRIPTION = "The average per-second number of ";
    private static final String COMMIT_DESCRIPTION = "commit calls";
    private static final String COMMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;
    private static final String COMMIT_RATE_DESCRIPTION = RATE_DESCRIPTION + COMMIT_DESCRIPTION;
    private static final String CREATE_TASK_DESCRIPTION = "newly created tasks";
    private static final String CREATE_TASK_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + CREATE_TASK_DESCRIPTION;
    private static final String CREATE_TASK_RATE_DESCRIPTION = RATE_DESCRIPTION + CREATE_TASK_DESCRIPTION;
    private static final String CLOSE_TASK_DESCRIPTION = "closed tasks";
    private static final String CLOSE_TASK_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
    private static final String CLOSE_TASK_RATE_DESCRIPTION = RATE_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
    private static final String POLL_DESCRIPTION = "poll calls";
    private static final String POLL_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + POLL_DESCRIPTION;
    private static final String POLL_RATE_DESCRIPTION = RATE_DESCRIPTION + POLL_DESCRIPTION;
    private static final String PROCESS_DESCRIPTION = "process calls";
    private static final String PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATE_DESCRIPTION = RATE_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PUNCTUATE_DESCRIPTION = "punctuate calls";
    private static final String PUNCTUATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_RATE_DESCRIPTION = RATE_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String SKIP_RECORDS_DESCRIPTION = "skipped records";
    private static final String SKIP_RECORD_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + SKIP_RECORDS_DESCRIPTION;
    private static final String SKIP_RECORD_RATE_DESCRIPTION = RATE_DESCRIPTION + SKIP_RECORDS_DESCRIPTION;
    private static final String COMMIT_OVER_TASKS_DESCRIPTION = "commit calls over all tasks";
    private static final String COMMIT_OVER_TASKS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
    private static final String COMMIT_OVER_TASKS_RATE_DESCRIPTION = RATE_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;

    private static final String COMMIT_LATENCY = COMMIT + LATENCY_SUFFIX;
    private static final String POLL_LATENCY = POLL + LATENCY_SUFFIX;
    private static final String PROCESS_LATENCY = PROCESS + LATENCY_SUFFIX;
    private static final String PUNCTUATE_LATENCY = PUNCTUATE + LATENCY_SUFFIX;

    public static Sensor createTaskSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor createTaskSensor = streamsMetrics.threadLevelSensor(CREATE_TASK, RecordingLevel.INFO);
        addInvocationRateAndCountToSensor(
            createTaskSensor,
            getThreadLevelGroup(streamsMetrics),
            streamsMetrics.threadLevelTagMap(),
            CREATE_TASK,
            CREATE_TASK_TOTAL_DESCRIPTION,
            CREATE_TASK_RATE_DESCRIPTION
        );
        return createTaskSensor;
    }

    public static Sensor closeTaskSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor closeTaskSensor = streamsMetrics.threadLevelSensor(CLOSE_TASK, RecordingLevel.INFO);
        addInvocationRateAndCountToSensor(
            closeTaskSensor,
            getThreadLevelGroup(streamsMetrics),
            streamsMetrics.threadLevelTagMap(),
            CLOSE_TASK,
            CLOSE_TASK_TOTAL_DESCRIPTION,
            CLOSE_TASK_RATE_DESCRIPTION
        );
        return closeTaskSensor;
    }

    public static Sensor commitSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor commitSensor = streamsMetrics.threadLevelSensor(COMMIT, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap();
        final String threadLevelGroup = getThreadLevelGroup(streamsMetrics);
        addAvgAndMaxToSensor(commitSensor, threadLevelGroup, tagMap, COMMIT_LATENCY);
        addInvocationRateAndCountToSensor(
            commitSensor,
            threadLevelGroup,
            tagMap,
            COMMIT,
            COMMIT_TOTAL_DESCRIPTION,
            COMMIT_RATE_DESCRIPTION
        );
        return commitSensor;
    }

    public static Sensor pollSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor pollSensor = streamsMetrics.threadLevelSensor(POLL, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap();
        final String threadLevelGroup = getThreadLevelGroup(streamsMetrics);
        addAvgAndMaxToSensor(pollSensor, threadLevelGroup, tagMap, POLL_LATENCY);
        addInvocationRateAndCountToSensor(
            pollSensor,
            threadLevelGroup,
            tagMap,
            POLL,
            POLL_TOTAL_DESCRIPTION,
            POLL_RATE_DESCRIPTION
        );
        return pollSensor;
    }

    public static Sensor processSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor processSensor = streamsMetrics.threadLevelSensor(PROCESS, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap();
        final String threadLevelGroup = getThreadLevelGroup(streamsMetrics);
        addAvgAndMaxToSensor(processSensor, threadLevelGroup, tagMap, PROCESS_LATENCY);
        addInvocationRateAndCountToSensor(
            processSensor,
            threadLevelGroup,
            tagMap,
            PROCESS,
            PROCESS_TOTAL_DESCRIPTION,
            PROCESS_RATE_DESCRIPTION
        );
        return processSensor;
    }

    public static Sensor punctuateSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor punctuateSensor = streamsMetrics.threadLevelSensor(PUNCTUATE, Sensor.RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap();
        final String threadLevelGroup = getThreadLevelGroup(streamsMetrics);
        addAvgAndMaxToSensor(punctuateSensor, threadLevelGroup, tagMap, PUNCTUATE_LATENCY);
        addInvocationRateAndCountToSensor(
            punctuateSensor,
            threadLevelGroup,
            tagMap,
            PUNCTUATE,
            PUNCTUATE_TOTAL_DESCRIPTION,
            PUNCTUATE_RATE_DESCRIPTION
        );
        return punctuateSensor;
    }

    public static Sensor skipRecordSensor(final StreamsMetricsImpl streamsMetrics) {
        final Sensor skippedRecordsSensor =
            streamsMetrics.threadLevelSensor(SKIP_RECORD, Sensor.RecordingLevel.INFO);
        addInvocationRateAndCountToSensor(
            skippedRecordsSensor,
            getThreadLevelGroup(streamsMetrics),
            streamsMetrics.threadLevelTagMap(),
            SKIP_RECORD,
            SKIP_RECORD_TOTAL_DESCRIPTION,
            SKIP_RECORD_RATE_DESCRIPTION
        );
        return skippedRecordsSensor;
    }

    private static String getThreadLevelGroup(final StreamsMetricsImpl streamsMetrics) {
        return streamsMetrics.version() == Version.LATEST ? THREAD_LEVEL_GROUP : THREAD_LEVEL_GROUP_0100_TO_23;
    }

    public static Optional<Sensor> commitOverTasksSensor(final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.FROM_100_TO_23) {
            final Sensor commitOverTasksSensor = streamsMetrics.threadLevelSensor(COMMIT, Sensor.RecordingLevel.DEBUG);
            final Map<String, String> tagMap = streamsMetrics.threadLevelTagMap(TASK_ID_TAG, ROLLUP_VALUE);
            addAvgAndMaxToSensor(commitOverTasksSensor, TASK_LEVEL_GROUP, tagMap, COMMIT_LATENCY);
            addInvocationRateAndCountToSensor(
                commitOverTasksSensor,
                TASK_LEVEL_GROUP,
                tagMap,
                COMMIT,
                COMMIT_OVER_TASKS_TOTAL_DESCRIPTION,
                COMMIT_OVER_TASKS_RATE_DESCRIPTION
            );
            return Optional.of(commitOverTasksSensor);
        }
        return Optional.empty();
    }
}
