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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAssignmentCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.metrics.ConsumerRebalanceMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryProvider;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamsMembershipManager implements RequestManager {

    private static class LocalAssignment {
        public static final long NONE_EPOCH = -1;
        public static final LocalAssignment NONE = new LocalAssignment(
            NONE_EPOCH,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        public final long localEpoch;
        public final Map<String, SortedSet<Integer>> activeTasks;
        public final Map<String, SortedSet<Integer>> standbyTasks;
        public final Map<String, SortedSet<Integer>> warmupTasks;

        public LocalAssignment(final long localEpoch,
                               final Map<String, SortedSet<Integer>> activeTasks,
                               final Map<String, SortedSet<Integer>> standbyTasks,
                               final Map<String, SortedSet<Integer>> warmupTasks) {
            this.localEpoch = localEpoch;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.warmupTasks = warmupTasks;
            if (localEpoch == NONE_EPOCH &&
                (!activeTasks.isEmpty() || !standbyTasks.isEmpty() || !warmupTasks.isEmpty())) {
                throw new IllegalArgumentException("Local epoch must be set if tasks are assigned.");
            }
        }

        Optional<LocalAssignment> updateWith(final Map<String, SortedSet<Integer>> activeTasks,
                                             final Map<String, SortedSet<Integer>> standbyTasks,
                                             final Map<String, SortedSet<Integer>> warmupTasks) {
            if (localEpoch != NONE_EPOCH) {
                if (activeTasks.equals(this.activeTasks) &&
                    standbyTasks.equals(this.standbyTasks) &&
                    warmupTasks.equals(this.warmupTasks)) {

                    return Optional.empty();
                }
            }

            // Bump local epoch and replace assignment
            long nextLocalEpoch = localEpoch + 1;
            return Optional.of(new LocalAssignment(nextLocalEpoch, activeTasks, standbyTasks, warmupTasks));
        }

        @Override
        public String toString() {
            return "LocalAssignment{" +
                "localEpoch=" + localEpoch +
                ", activeTasks=" + activeTasks +
                ", standbyTasks=" + standbyTasks +
                ", warmupTasks=" + warmupTasks +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LocalAssignment that = (LocalAssignment) o;
            return localEpoch == that.localEpoch &&
                Objects.equals(activeTasks, that.activeTasks) &&
                Objects.equals(standbyTasks, that.standbyTasks) &&
                Objects.equals(warmupTasks, that.warmupTasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(localEpoch, activeTasks, standbyTasks, warmupTasks);
        }
    }

    /**
     * TopicPartition comparator based on topic name and partition.
     */
    static final Utils.TopicPartitionComparator TOPIC_PARTITION_COMPARATOR = new Utils.TopicPartitionComparator();

    /**
     * TopicIdPartition comparator based on topic name and partition (ignoring topic ID while sorting,
     * as this is sorted mainly for logging purposes).
     */
    static final Utils.TopicIdPartitionComparator TOPIC_ID_PARTITION_COMPARATOR = new Utils.TopicIdPartitionComparator();

    /**
     * Logger.
     */
    private final Logger log;

    private final StreamsAssignmentInterface streamsAssignmentInterface;

    /**
     * Subscription state object holding the current assignment the member has for the topics it
     * subscribed to.
     */
    private final SubscriptionState subscriptions;

    private final ConsumerMetadata consumerMetadata;

    private final BackgroundEventHandler backgroundEventHandler;

    private final Map<String, Uuid> assignedTopicIdCache = new HashMap<>();

    /**
     * Local cache of assigned topic IDs and names. Topics are added here when received in a
     * target assignment, as we discover their names in the Metadata cache, and removed when the
     * topic is not in the subscription anymore. The purpose of this cache is to avoid metadata
     * requests in cases where a currently assigned topic is in the target assignment (new
     * partition assigned, or revoked), but it is not present the Metadata cache at that moment.
     * The cache is cleared when the subscription changes ({@link #transitionToJoining()}, the
     * member fails ({@link #transitionToFatal()} or leaves the group ({@link #leaveGroup()}).
     */
    private final Map<Uuid, String> assignedTopicNamesCache = new HashMap<>();

    private MemberState state;

    /**
     * Member ID assigned by the server to the member, received in a heartbeat response when
     * joining the group specified in {@link #groupId}
     */
    private String memberId = "";

    /**
     * Current epoch of the member. It will be set to 0 by the member, and provided to the server
     * on the heartbeat request, to join the group. It will be then maintained by the server,
     * incremented as the member reconciles and acknowledges the assignments it receives. It will
     * be reset to 0 if the member gets fenced.
     */
    private int memberEpoch = 0;

    /**
     * If the member is currently leaving the group after a call to {@link #leaveGroup()}}, this
     * will have a future that will complete when the ongoing leave operation completes
     * (callbacks executed and heartbeat request to leave is sent out). This will be empty is the
     * member is not leaving.
     */
    private Optional<CompletableFuture<Void>> leaveGroupInProgress = Optional.empty();

    /**
     * If there is a reconciliation running (triggering commit, callbacks) for the
     * assignmentReadyToReconcile. This will be true if {@link #maybeReconcile()} has been triggered
     * after receiving a heartbeat response, or a metadata update.
     */
    private boolean reconciliationInProgress;

    /**
     * True if a reconciliation is in progress and the member rejoined the group since the start
     * of the reconciliation. Used to know that the reconciliation in progress should be
     * interrupted and not be applied.
     */
    private boolean rejoinedWhileReconciliationInProgress;

    /**
     * Registered listeners that will be notified whenever the memberID/epoch gets updated (valid
     * values received from the broker, or values cleared due to member leaving the group, getting
     * fenced or failing).
     */
    private final List<MemberStateListener> stateUpdatesListeners;

    /**
     * Optional client telemetry reporter which sends client telemetry data to the broker. This
     * will be empty if the client telemetry feature is not enabled. This is provided to update
     * the group member id label when the member joins the group.
     */
    protected final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    private LocalAssignment targetAssignment = LocalAssignment.NONE;

    private LocalAssignment currentAssignment = LocalAssignment.NONE;

    /**
     * Measures successful rebalance latency and number of failed rebalances.
     */
    private final RebalanceMetricsManager metricsManager;

    private final Time time;

    public StreamsMembershipManager(final StreamsAssignmentInterface streamsAssignmentInterface,
                                    final ConsumerMetadata metadata,
                                    final SubscriptionState subscriptions,
                                    final LogContext logContext,
                                    final Optional<ClientTelemetryReporter> clientTelemetryReporter,
                                    final BackgroundEventHandler backgroundEventHandler,
                                    final Time time,
                                    final Metrics metrics) {
        log = logContext.logger(StreamsMembershipManager.class);
        this.state = MemberState.UNSUBSCRIBED;
        this.streamsAssignmentInterface = streamsAssignmentInterface;
        this.consumerMetadata = metadata;
        this.subscriptions = subscriptions;
        this.stateUpdatesListeners = new ArrayList<>();
        this.clientTelemetryReporter = clientTelemetryReporter;
        this.backgroundEventHandler = backgroundEventHandler;
        metricsManager = new ConsumerRebalanceMetricsManager(metrics);
        this.time = time;
    }

    public void onHeartbeatSuccess(StreamsGroupHeartbeatResponse response) {
        StreamsGroupHeartbeatResponseData responseData = response.data();
        throwIfUnexpectedError(responseData);
        if (state == MemberState.LEAVING) {
            log.debug("Ignoring heartbeat response received from broker. Member {} with epoch {} is " +
                "already leaving the group.", memberId, memberEpoch);
            return;
        }
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            log.debug("Member {} with epoch {} received a successful response to the heartbeat " +
                "to leave the group and completed the leave operation. ", memberId, memberEpoch);
            return;
        }
        if (isNotInGroup()) {
            log.debug("Ignoring heartbeat response received from broker. Member {} is in {} state" +
                " so it's not a member of the group. ", memberId, state);
            return;
        }

        // Update the group member id label in the client telemetry reporter if the member id has
        // changed. Initially the member id is empty, and it is updated when the member joins the
        // group. This is done here to avoid updating the label on every heartbeat response. Also
        // check if the member id is null, as the schema defines it as nullable.
        if (responseData.memberId() != null && !responseData.memberId().equals(memberId)) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.updateMetricsLabels(
                Collections.singletonMap(ClientTelemetryProvider.GROUP_MEMBER_ID, responseData.memberId())));
        }

        memberId = responseData.memberId();
        updateMemberEpoch(responseData.memberEpoch());

        final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks = responseData.activeTasks();
        final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks = responseData.standbyTasks();
        final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks = responseData.warmupTasks();

        if (activeTasks != null && standbyTasks != null && warmupTasks != null) {

            if (!state.canHandleNewAssignment()) {
                log.debug("Ignoring new assignment: active tasks {}, standby tasks {}, and warm-up tasks {} received " +
                        "from server because member is in {} state.",
                        activeTasks, standbyTasks, warmupTasks, state);
                return;
            }

            processAssignmentReceived(
                toTasksAssignment(activeTasks),
                toTasksAssignment(standbyTasks),
                toTasksAssignment(warmupTasks)
            );
        } else {
            if (responseData.activeTasks() != null ||
                responseData.standbyTasks() != null ||
                responseData.warmupTasks() != null) {

                throw new IllegalStateException("Invalid response data, task collections must be all null or all non-null: "
                    + responseData);
            }
        }
    }

    private void throwIfUnexpectedError(StreamsGroupHeartbeatResponseData responseData) {
        if (responseData.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                Errors.forCode(responseData.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * @return True if the consumer is not a member of the group.
     */
    private boolean isNotInGroup() {
        return state == MemberState.UNSUBSCRIBED ||
            state == MemberState.FENCED ||
            state == MemberState.FATAL ||
            state == MemberState.STALE;
    }

    /**
     * Complete the leave in progress (if any). This is expected to be used to complete the leave
     * in progress when a member receives the response to the leave heartbeat.
     */
    private boolean maybeCompleteLeaveInProgress() {
        if (leaveGroupInProgress.isPresent()) {
            leaveGroupInProgress.get().complete(null);
            leaveGroupInProgress = Optional.empty();
            return true;
        }
        return false;
    }

    private void updateMemberEpoch(int newEpoch) {
        boolean newEpochReceived = this.memberEpoch != newEpoch;
        this.memberEpoch = newEpoch;
        // Simply notify based on epoch change only, given that the member will never receive a
        // new member ID without an epoch (member ID is only assigned when it joins the group).
        if (newEpochReceived) {
            if (memberEpoch > 0) {
                notifyEpochChange(Optional.of(memberEpoch), Optional.ofNullable(memberId));
            } else {
                notifyEpochChange(Optional.empty(), Optional.empty());
            }
        }
    }

    /**
     * Call all listeners that are registered to get notified when the member epoch is updated.
     * This also includes the latest member ID in the notification. If the member fails or leaves
     * the group, this will be invoked with empty epoch and member ID.
     */
    void notifyEpochChange(Optional<Integer> epoch,
                           Optional<String> memberId) {
        stateUpdatesListeners.forEach(stateListener -> stateListener.onMemberEpochUpdated(epoch, memberId));
    }

    private static Set<StreamsAssignmentInterface.TaskId> toTaskIdSet(List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds) {
        // ToDo: consider using a sorted set
        Set<StreamsAssignmentInterface.TaskId> taskIdSet = new HashSet<>();
        for (final StreamsGroupHeartbeatResponseData.TaskIds taskIdsOfSubtopology : taskIds) {
            final String subtopology = taskIdsOfSubtopology.subtopology();
            final List<Integer> partitions = taskIdsOfSubtopology.partitions();
            for (final int partition : partitions) {
                taskIdSet.add(new StreamsAssignmentInterface.TaskId(subtopology, partition));
            }
        }
        return taskIdSet;
    }

    private static SortedSet<StreamsAssignmentInterface.TaskId> toTaskIdSet(final Map<String, SortedSet<Integer>> tasks) {
        SortedSet<StreamsAssignmentInterface.TaskId> taskIdSet = new TreeSet<>();
        for (final Map.Entry<String, SortedSet<Integer>> task : tasks.entrySet()) {
            final String subtopologyId = task.getKey();
            final SortedSet<Integer> partitions = task.getValue();
            for (final int partition : partitions) {
                taskIdSet.add(new StreamsAssignmentInterface.TaskId(subtopologyId, partition));
            }
        }
        return taskIdSet;
    }

    private Map<Uuid, SortedSet<Integer>> computeTargetAssignmentForConsumer(final StreamsGroupHeartbeatResponseData streamsGroupHeartbeatResponseData) {
        Map<Uuid, SortedSet<Integer>> consumerAssignment = new HashMap<>();
        streamsGroupHeartbeatResponseData.activeTasks().forEach(taskId -> Stream.concat(
                streamsAssignmentInterface.subtopologyMap().get(taskId.subtopology()).sourceTopics.stream(),
                streamsAssignmentInterface.subtopologyMap().get(taskId.subtopology()).repartitionSourceTopics.keySet().stream()
            )
            .forEach(topic -> {
                final Optional<Uuid> uuid = findTopicIdInGlobalOrLocalCache(topic);
                uuid.ifPresent(value -> consumerAssignment.computeIfAbsent(value, ignored -> new TreeSet<>())
                    .addAll(taskId.partitions()));
            }));
        return consumerAssignment;
    }

    private Map<String, SortedSet<Integer>> toTasksAssignment(final List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds) {
        return taskIds.stream()
            .collect(Collectors.toMap(StreamsGroupHeartbeatResponseData.TaskIds::subtopology, taskId -> new TreeSet<>(taskId.partitions())));
    }

    private Optional<Uuid> findTopicIdInGlobalOrLocalCache(String topicName) {
        Uuid idFromMetadataCache = consumerMetadata.topicIds().getOrDefault(topicName, null);
        if (idFromMetadataCache != null) {
            // Add topic name to local cache, so it can be reused if included in a next target
            // assignment if metadata cache not available.
            assignedTopicIdCache.put(topicName, idFromMetadataCache);
            return Optional.of(idFromMetadataCache);
        } else {
            Uuid idFromLocalCache = assignedTopicIdCache.getOrDefault(topicName, null);
            return Optional.ofNullable(idFromLocalCache);
        }
    }

    /**
     * This will process the assignment received if it is different from the member's current
     * assignment. If a new assignment is received, this will make sure reconciliation is attempted
     * on the next call of `poll`. If another reconciliation is currently in process, the first `poll`
     * after that reconciliation will trigger the new reconciliation.
     *
     * @param activeTasks Target active tasks assignment received from the broker.
     * @param standbyTasks Target standby tasks assignment received from the broker.
     * @param warmupTasks Target warm-up tasks assignment received from the broker.
     */
    private void processAssignmentReceived(Map<String, SortedSet<Integer>> activeTasks,
                                           Map<String, SortedSet<Integer>> standbyTasks,
                                           Map<String, SortedSet<Integer>> warmupTasks) {
        replaceTargetAssignmentWithNewAssignment(activeTasks, standbyTasks, warmupTasks);
        if (!targetAssignmentReconciled()) {
            transitionTo(MemberState.RECONCILING);
        } else {
            log.debug("Target assignment {} received from the broker is equals to the member " +
                    "current assignment {}. Nothing to reconcile.",
                targetAssignment, currentAssignment);
            // Make sure we transition the member back to STABLE if it was RECONCILING (ex.
            // member was RECONCILING unresolved assignments that were just removed by the
            // broker), or JOINING (member joining received empty assignment).
            if (state == MemberState.RECONCILING || state == MemberState.JOINING) {
                transitionTo(MemberState.STABLE);
            }
        }
    }

    /**
     * @return True if there are no assignments waiting to be resolved from metadata or reconciled.
     */
    private boolean targetAssignmentReconciled() {
        return currentAssignment.equals(targetAssignment);
    }

    /**
     * Overwrite the target assignment with the new target assignment.
     *
     * @param activeTasks Target active tasks assignment received from the broker.
     * @param standbyTasks Target standby tasks assignment received from the broker.
     * @param warmupTasks Target warm-up tasks assignment received from the broker.
     */
    private void replaceTargetAssignmentWithNewAssignment(Map<String, SortedSet<Integer>> activeTasks,
                                                          Map<String, SortedSet<Integer>> standbyTasks,
                                                          Map<String, SortedSet<Integer>> warmupTasks) {
        targetAssignment.updateWith(activeTasks, standbyTasks, warmupTasks)
            .ifPresent(updatedAssignment -> {
                log.debug("Target assignment updated from {} to {}. Member will reconcile it on the next poll.",
                    targetAssignment, updatedAssignment);
                targetAssignment = updatedAssignment;
            });
    }

    /**
     * Update the member state, setting it to the nextState only if it is a valid transition.
     *
     * @throws IllegalStateException If transitioning from the member {@link #state} to the
     *                               nextState is not allowed as defined in {@link MemberState}.
     */
    protected void transitionTo(MemberState nextState) {
        if (!state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
            throw new IllegalStateException(String.format("Invalid state transition from %s to %s",
                state, nextState));
        }

        if (isCompletingRebalance(state, nextState)) {
            metricsManager.recordRebalanceEnded(time.milliseconds());
        }
        if (isStartingRebalance(state, nextState)) {
            metricsManager.recordRebalanceStarted(time.milliseconds());
        }

        log.info("Member {} with epoch {} transitioned from {} to {}.", memberIdInfoForLog(), memberEpoch, state, nextState);
        this.state = nextState;
    }

    private static boolean isCompletingRebalance(MemberState currentState, MemberState nextState) {
        return currentState == MemberState.RECONCILING &&
            (nextState == MemberState.STABLE || nextState == MemberState.ACKNOWLEDGING);
    }

    private static boolean isStartingRebalance(MemberState currentState, MemberState nextState) {
        return currentState != MemberState.RECONCILING && nextState == MemberState.RECONCILING;
    }

    private String memberIdInfoForLog() {
        return (memberId == null || memberId.isEmpty()) ? "<no ID>" : memberId;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (state == MemberState.RECONCILING) {
            maybeReconcile();
        }
        return NetworkClientDelegate.PollResult.EMPTY;
    }

    private void maybeReconcile() {
        if (targetAssignmentReconciled()) {
            log.trace("Ignoring reconciliation attempt. Target assignment is equal to the " +
                "current assignment.");
            return;
        }
        if (reconciliationInProgress) {
            log.trace("Ignoring reconciliation attempt. Another reconciliation is already in progress. Assignment " +
                targetAssignment + " will be handled in the next reconciliation loop.");
            return;
        }

        markReconciliationInProgress();

        SortedSet<StreamsAssignmentInterface.TaskId> assignedActiveTasks = toTaskIdSet(targetAssignment.activeTasks);

        log.info("Assigned tasks with local epoch {}\n" +
                "\tMember:                        {}\n" +
                "\tActive tasks:                  {}\n",
            targetAssignment.localEpoch,
            memberIdInfoForLog(),
            assignedActiveTasks
        );

        SortedSet<TopicPartition> ownedTopicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        ownedTopicPartitions.addAll(subscriptions.assignedPartitions());
        SortedSet<TopicPartition> assignedTopicPartitions = topicPartitionsForActiveTasks(targetAssignment.activeTasks);
        SortedSet<TopicPartition> assignedPartitionsNotPreviouslyOwned = assignedPartitionsNotPreviouslyOwned(assignedTopicPartitions, ownedTopicPartitions);

//        updateSubscriptionAwaitingCallback()
        final StreamsOnAssignmentCallbackNeededEvent onAssignmentCallbackNeededEvent =
            new StreamsOnAssignmentCallbackNeededEvent(new StreamsAssignmentInterface.Assignment(
                assignedActiveTasks,
                Collections.emptySet(),
                Collections.emptySet()
            ));
        CompletableFuture<Void> onTasksAssignmentDone = onAssignmentCallbackNeededEvent.future();
        backgroundEventHandler.add(onAssignmentCallbackNeededEvent);
        onTasksAssignmentDone.whenComplete((__, callbackError) -> {
            if (callbackError != null) {
                log.error("Reconciliation failed: onTasksAssignment callback invocation failed for tasks {}",
                    currentAssignment, callbackError);
                markReconciliationCompleted();
            } else {
                if (reconciliationInProgress && !maybeAbortReconciliation()) {
                    subscriptions.enablePartitionsAwaitingCallback(assignedPartitionsNotPreviouslyOwned);

                    currentAssignment = targetAssignment;
                    transitionTo(MemberState.ACKNOWLEDGING);
                    markReconciliationCompleted();
                }
            }
        });


        onTasksAssignmentDone.whenComplete((__, error) -> {
                if (error != null) {
                } else {
                    if (reconciliationInProgress && !maybeAbortReconciliation()) {
                        currentAssignment = targetAssignment;
                        transitionTo(MemberState.ACKNOWLEDGING);
                        markReconciliationCompleted();
                    }
                }
            }
        );
    }

    private SortedSet<TopicPartition> assignedPartitionsNotPreviouslyOwned(final SortedSet<TopicPartition> assignedTopicPartitions,
                                                                           final SortedSet<TopicPartition> ownedTopicPartitions) {
        SortedSet<TopicPartition> assignedPartitionsNotPreviouslyOwned = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        assignedPartitionsNotPreviouslyOwned.addAll(assignedTopicPartitions);
        assignedPartitionsNotPreviouslyOwned.removeAll(ownedTopicPartitions);
        return assignedPartitionsNotPreviouslyOwned;
    }

    private SortedSet<TopicPartition> topicPartitionsForActiveTasks(final Map<String, SortedSet<Integer>> activeTasks) {
        final SortedSet<TopicPartition> topicPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        activeTasks.forEach((subtopologyId, partitionIds) ->
            Stream.concat(
                streamsAssignmentInterface.subtopologyMap().get(subtopologyId).sourceTopics.stream(),
                streamsAssignmentInterface.subtopologyMap().get(subtopologyId).repartitionSourceTopics.keySet().stream()
            ).forEach(topic -> {
                for (final int partitionId : partitionIds) {
                    topicPartitions.add(new TopicPartition(topic, partitionId));
                }
            })
        );
        return topicPartitions;
    }

    private void markReconciliationCompleted() {
        reconciliationInProgress = false;
        rejoinedWhileReconciliationInProgress = false;
    }

    private boolean maybeAbortReconciliation() {
        boolean shouldAbort = state != MemberState.RECONCILING || rejoinedWhileReconciliationInProgress;
        if (shouldAbort) {
            String reason = rejoinedWhileReconciliationInProgress ?
                "the member has re-joined the group" :
                "the member already transitioned out of the reconciling state into " + state;
            log.info("Interrupting reconciliation that is not relevant anymore because " + reason);
            markReconciliationCompleted();
        }
        return shouldAbort;
    }

    /**
     * Trigger onPartitionsRevoked callbacks if any partitions where revoked. If it succeeds,
     * proceed to trigger the onPartitionsAssigned (even if no new partitions were added), and
     * then complete the reconciliation by updating the assignment and making the appropriate state
     * transition. Note that if any of the 2 callbacks fails, the reconciliation should fail.
     */
    private void revokeAndAssign(AbstractMembershipManager.LocalAssignment resolvedAssignment,
                                 SortedSet<TopicIdPartition> assignedTopicIdPartitions,
                                 SortedSet<TopicPartition> revokedPartitions,
                                 SortedSet<TopicPartition> addedPartitions) {
        if (!revokedPartitions.isEmpty()) {
            revokePartitions(revokedPartitions);
        }

        assignPartitions(assignedTopicIdPartitions, addedPartitions);

        CompletableFuture<Void> reconciliationResult
            revocationResult.thenCompose(__ -> {
                if (!maybeAbortReconciliation()) {
                    // Apply assignment
                    return assignPartitions(assignedTopicIdPartitions, addedPartitions);
                }
                return CompletableFuture.completedFuture(null);
            });

        reconciliationResult.whenComplete((__, error) -> {
            if (error != null) {
                // Leaving member in RECONCILING state after callbacks fail. The member
                // won't send the ack, and the expectation is that the broker will kick the
                // member out of the group after the reconciliation commit timeout expires, leading to a
                // RECONCILING -> FENCED transition.
                log.error("Reconciliation failed.", error);
                markReconciliationCompleted();
            } else {
                if (reconciliationInProgress && !maybeAbortReconciliation()) {
                    currentAssignment = resolvedAssignment;

                    signalReconciliationCompleting();

                    // Make assignment effective on the broker by transitioning to send acknowledge.
                    transitionTo(MemberState.ACKNOWLEDGING);
                    markReconciliationCompleted();
                }
            }
        });
    }

    private void revokePartitions(Set<TopicPartition> partitionsToRevoke) {
        // Ensure the set of partitions to revoke are still assigned
        Set<TopicPartition> revokedPartitions = new HashSet<>(partitionsToRevoke);
        revokedPartitions.retainAll(subscriptions.assignedPartitions());
        log.info("Revoking previously assigned partitions {}", revokedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));

        signalPartitionsBeingRevoked(revokedPartitions);

        // Mark partitions as pending revocation to stop fetching from the partitions (no new
        // fetches sent out, and no in-flight fetches responses processed).
        markPendingRevocationToPauseFetching(revokedPartitions);

        // Future that will complete when the revocation completes (including offset commit
        // request and user callback execution).
        CompletableFuture<Void> revocationResult = new CompletableFuture<>();

        // At this point we expect to be in a middle of a revocation triggered from RECONCILING
        // or PREPARE_LEAVING, but it could be the case that the member received a fatal error
        // while waiting for the commit to complete. Check if that's the case and abort the
        // revocation.
        if (state == MemberState.FATAL) {
            String errorMsg = String.format("Member %s with epoch %s received a fatal error " +
                "while waiting for a revocation commit to complete. Will abort revocation " +
                "without triggering user callback.", memberIdInfoForLog(), memberEpoch);
            log.debug(errorMsg);
            revocationResult.completeExceptionally(new KafkaException(errorMsg));
            return revocationResult;
        }

        CompletableFuture<Void> userCallbackResult = signalPartitionsRevoked(revokedPartitions);
        userCallbackResult.whenComplete((callbackResult, callbackError) -> {
            if (callbackError != null) {
                log.error("onPartitionsRevoked callback invocation failed for partitions {}",
                    revokedPartitions, callbackError);
                revocationResult.completeExceptionally(callbackError);
            } else {
                revocationResult.complete(null);
            }

        });
        return revocationResult;
    }

    private CompletableFuture<Void> assignPartitions(SortedSet<TopicIdPartition> assignedPartitions,
                                                     SortedSet<TopicPartition> addedPartitions) {

        // Update assignment in the subscription state, and ensure that no fetching or positions
        // initialization happens for the newly added partitions while the callback runs.
        updateSubscriptionAwaitingCallback(assignedPartitions, addedPartitions);

        // Invoke user call back.
        CompletableFuture<Void> result = signalPartitionsAssigned(addedPartitions);
        result.whenComplete((__, exception) -> {
            if (exception == null) {
                // Enable newly added partitions to start fetching and updating positions for them.
                subscriptions.enablePartitionsAwaitingCallback(addedPartitions);
            } else {
                // Keeping newly added partitions as non-fetchable after the callback failure.
                // They will be retried on the next reconciliation loop, until it succeeds or the
                // broker removes them from the assignment.
                if (!addedPartitions.isEmpty()) {
                    log.warn("Leaving newly assigned partitions {} marked as non-fetchable and not " +
                            "requiring initializing positions after onPartitionsAssigned callback failed.",
                        addedPartitions, exception);
                }
            }
        });

        // Clear topic names cache, removing topics that are not assigned to the member anymore.
        Set<String> assignedTopics = assignedPartitions.stream().map(TopicIdPartition::topic).collect(Collectors.toSet());
        assignedTopicNamesCache.values().retainAll(assignedTopics);

        return result;
    }

    /**
     * Build set of TopicIdPartition (topic ID, topic name and partition id) from the target assignment
     * received from the broker (topic IDs and list of partitions).
     *
     * <p>
     * This will:
     *
     * <ol type="1">
     *     <li>Try to find topic names in the metadata cache</li>
     *     <li>For topics not found in metadata, try to find names in the local topic names cache
     *     (contains topic id and names currently assigned and resolved)</li>
     *     <li>If there are topics that are not in metadata cache or in the local cache
     *     of topic names assigned to this member, request a metadata update, and continue
     *     resolving names as the cache is updated.
     *     </li>
     * </ol>
     */
    private SortedSet<TopicIdPartition> findResolvableAssignmentAndTriggerMetadataUpdate() {
        final SortedSet<TopicIdPartition> assignmentReadyToReconcile = new TreeSet<>(TOPIC_ID_PARTITION_COMPARATOR);
        final HashMap<Uuid, SortedSet<Integer>> unresolved = new HashMap<>(targetAssignment.partitions);

        // Try to resolve topic names from metadata cache or subscription cache, and move
        // assignments from the "unresolved" collection, to the "assignmentReadyToReconcile" one.
        Iterator<Map.Entry<Uuid, SortedSet<Integer>>> it = unresolved.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Uuid, SortedSet<Integer>> e = it.next();
            Uuid topicId = e.getKey();
            SortedSet<Integer> topicPartitions = e.getValue();

            Optional<String> nameFromMetadata = findTopicNameInGlobalOrLocalCache(topicId);
            nameFromMetadata.ifPresent(resolvedTopicName -> {
                // Name resolved, so assignment is ready for reconciliation.
                topicPartitions.forEach(tp ->
                    assignmentReadyToReconcile.add(new TopicIdPartition(topicId, tp, resolvedTopicName))
                );
                it.remove();
            });
        }

        if (!unresolved.isEmpty()) {
            log.debug("Topic Ids {} received in target assignment were not found in metadata and " +
                "are not currently assigned. Requesting a metadata update now to resolve " +
                "topic names.", unresolved.keySet());
            consumerMetadata.requestUpdate(true);
        }

        return assignmentReadyToReconcile;
    }

    /**
     * Look for topic in the global metadata cache. If found, add it to the local cache and
     * return it. If not found, look for it in the local metadata cache. Return empty if not
     * found in any of the two.
     */
    private Optional<String> findTopicNameInGlobalOrLocalCache(Uuid topicId) {
        String nameFromMetadataCache = consumerMetadata.topicNames().getOrDefault(topicId, null);
        if (nameFromMetadataCache != null) {
            // Add topic name to local cache, so it can be reused if included in a next target
            // assignment if metadata cache not available.
            assignedTopicNamesCache.put(topicId, nameFromMetadataCache);
            return Optional.of(nameFromMetadataCache);
        } else {
            // Topic ID was not found in metadata. Check if the topic name is in the local
            // cache of topics currently assigned. This will avoid a metadata request in the
            // case where the metadata cache may have been flushed right before the
            // revocation of a previously assigned topic.
            String nameFromSubscriptionCache = assignedTopicNamesCache.getOrDefault(topicId, null);
            return Optional.ofNullable(nameFromSubscriptionCache);
        }
    }

    /**
     * Build set of {@link TopicPartition} from the given set of {@link TopicIdPartition}.
     */
    private SortedSet<TopicPartition> toTopicPartitionSet(SortedSet<TopicIdPartition> topicIdPartitions) {
        SortedSet<TopicPartition> result = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        topicIdPartitions.forEach(topicIdPartition -> result.add(topicIdPartition.topicPartition()));
        return result;
    }

    /**
     *  Visible for testing.
     */
    private void markReconciliationInProgress() {
        reconciliationInProgress = true;
        rejoinedWhileReconciliationInProgress = false;
    }

    @Override
    public NetworkClientDelegate.PollResult pollOnClose(long currentTimeMs) {
        return RequestManager.super.pollOnClose(currentTimeMs);
    }

    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        return RequestManager.super.maximumTimeToWait(currentTimeMs);
    }

    @Override
    public void signalClose() {
        RequestManager.super.signalClose();
    }
}
