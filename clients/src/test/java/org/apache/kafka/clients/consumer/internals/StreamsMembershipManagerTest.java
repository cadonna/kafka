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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAssignmentCallbackNeededEvent;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class StreamsMembershipManagerTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;
    private static final int THROTTLE_TIME = 1;
    private static final LogContext LOG_CONTEXT = new LogContext();

    private Time time = new MockTime(0);
    private Metrics metrics = new Metrics(time);

    @Mock
    private ConsumerMetadata consumerMetadata;

    @Mock
    private SubscriptionState subscriptionState;

    @Mock
    private StreamsAssignmentInterface streamsAssignmentInterface;

    @Mock
    private ConsumerMembershipManager consumerMembershipManager;

    @Mock
    private BackgroundEventHandler backgroundEventHandler;

    @Test
    public void testOnHeartbeatSuccess() {
        final StreamsMembershipManager streamsMembershipManager = new StreamsMembershipManager(
            consumerMembershipManager,
            streamsAssignmentInterface,
            consumerMetadata,
            backgroundEventHandler
        );
        final String subtopologyId0 = "0";
        final String errorMessage = "false error";
        final int heartbeatIntervalMs = 1000;
        final List<Integer> assignedPartitions = Arrays.asList(0);
        final StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setThrottleTimeMs(THROTTLE_TIME)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(errorMessage)
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setHeartbeatIntervalMs(heartbeatIntervalMs)
            .setActiveTasks(Collections.singletonList(
                new StreamsGroupHeartbeatResponseData.TaskIds().setSubtopology(subtopologyId0).setPartitions(assignedPartitions)))
            .setStandbyTasks(Collections.emptyList())
            .setWarmupTasks(Collections.emptyList());
        final StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(responseData);
        final Uuid topicId = Uuid.randomUuid();
        final String topicName = "test_topic";
        final Map<Uuid, String> topicNames = mkMap(mkEntry(topicId, topicName));
        when(consumerMetadata.topicIds()).thenReturn(mkMap(mkEntry(topicName, topicId)));
        when(streamsAssignmentInterface.subtopologyMap()).thenReturn(
            mkMap(
                mkEntry(
                    subtopologyId0,
                    new StreamsAssignmentInterface.Subtopology(
                        mkSet(topicName),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    )
                )
            )
        );
        final ConsumerGroupHeartbeatResponseData.TopicPartitions topicPartitions = new ConsumerGroupHeartbeatResponseData.TopicPartitions()
            .setTopicId(topicId)
            .setPartitions(assignedPartitions);
        final ConsumerGroupHeartbeatResponseData.Assignment expectedConsumerAssignment = new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(Arrays.asList(topicPartitions));
        final StreamsAssignmentInterface.Assignment expectedStreamsAssignment = new StreamsAssignmentInterface.Assignment(
            assignedPartitions.stream()
                .map(partition -> new StreamsAssignmentInterface.TaskId(subtopologyId0, partition))
                .collect(Collectors.toSet()),
            Collections.emptySet(),
            Collections.emptySet()
        );

        streamsMembershipManager.onHeartbeatSuccess(response);
        streamsMembershipManager.poll(time.milliseconds());

        final AtomicReference<StreamsOnAssignmentCallbackNeededEvent> onAssignmentCallbackNeededEvent = new AtomicReference<>();
        verify(backgroundEventHandler).add(argThat(event -> {
            onAssignmentCallbackNeededEvent.set((StreamsOnAssignmentCallbackNeededEvent) event);
            return ((StreamsOnAssignmentCallbackNeededEvent) event).assignment().equals(expectedStreamsAssignment);
        }));
        verify(consumerMembershipManager, never()).onHeartbeatSuccess(any());
        onAssignmentCallbackNeededEvent.get().future().complete(null);
//        verify(consumerMembershipManager).onHeartbeatSuccess(argThat(consumerGroupHeartbeatResponse ->
//            consumerGroupHeartbeatResponse.data().throttleTimeMs() == THROTTLE_TIME &&
//            consumerGroupHeartbeatResponse.data().errorCode() == Errors.NONE.code() &&
//            consumerGroupHeartbeatResponse.data().errorMessage().equals(errorMessage) &&
//            consumerGroupHeartbeatResponse.data().memberId().equals(MEMBER_ID) &&
//            consumerGroupHeartbeatResponse.data().memberEpoch() == MEMBER_EPOCH &&
//            consumerGroupHeartbeatResponse.data().heartbeatIntervalMs() == heartbeatIntervalMs &&
//            consumerGroupHeartbeatResponse.data().assignment().equals(expectedConsumerAssignment)
//        ));
    }
}
