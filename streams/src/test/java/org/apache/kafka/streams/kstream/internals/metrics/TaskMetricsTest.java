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
package org.apache.kafka.streams.kstream.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamsMetricsImpl.class, Sensor.class})
public class TaskMetricsTest {

    private final static String TASK_ID = "test-task";

    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Sensor expectedSensor = createMock(Sensor.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");

    @Test
    public void shouldGetProcessLatencySensor() {
        mockStatic(StreamsMetricsImpl.class);
        final String operation = "process-latency";
        expect(streamsMetrics.version()).andReturn(Version.LATEST);
        expect(streamsMetrics.taskLevelSensor(TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(expectedSensor, TASK_LEVEL_GROUP, tagMap, operation);
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.processLatencySensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldNotGetProcessLatencySensor() {
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.version()).andReturn(Version.FROM_100_TO_23);
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.processLatencySensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, nullValue());
    }

    @Test
    public void shouldGetPunctuateSensor() {
        mockStatic(StreamsMetricsImpl.class);
        final String operation = "punctuate";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of punctuate calls";
        final String rateDescription = "The average per-second number of punctuate calls";
        expect(streamsMetrics.version()).andReturn(Version.LATEST);
        expect(streamsMetrics.taskLevelSensor(TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            totalDescription,
            rateDescription
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(expectedSensor, TASK_LEVEL_GROUP, tagMap, operationLatency);
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.punctuateSensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldNotGetPunctuateSensor() {
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.version()).andReturn(Version.FROM_100_TO_23);
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.punctuateSensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, nullValue());
    }

    @Test
    public void shouldGetCommitSensor() {
        mockStatic(StreamsMetricsImpl.class);
        final String operation = "commit";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of commit calls";
        final String rateDescription = "The average per-second number of commit calls";
        expect(streamsMetrics.taskLevelSensor(TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            totalDescription,
            rateDescription
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(expectedSensor, TASK_LEVEL_GROUP, tagMap, operationLatency);
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.commitSensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetEnforcedProcessingSensor() {
        mockStatic(StreamsMetricsImpl.class);
        final String operation = "enforced-processing";
        final String totalDescription = "The total number of occurrence of enforced-processing operations";
        final String rateDescription = "The average number of occurrence of enforced-processing operation per second";
        expect(streamsMetrics.taskLevelSensor(TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            totalDescription,
            rateDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.enforcedProcessingSensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetRecordLatenessSensor() {
        mockStatic(StreamsMetricsImpl.class);
        final String operation = "record-lateness";
        final String avgDescription = "The average observed lateness of records";
        final String maxDescription = "The max observed lateness of records";
        expect(streamsMetrics.taskLevelSensor(TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            avgDescription,
            maxDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.recordLatenessSensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetDroppedRecordsSensor() {
        mockStatic(StreamsMetricsImpl.class);
        final String operation = "dropped-records";
        final String totalDescription = "The total number of dropped records";
        final String rateDescription = "The average per-second number of dropped records";
        expect(streamsMetrics.taskLevelSensor(TASK_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            totalDescription,
            rateDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.droppedRecordsSensor(TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }
}