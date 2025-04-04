/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.QuadConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultSlotStatusSyncer}. */
class DefaultSlotStatusSyncerTest {
    private static final Duration TASK_MANAGER_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testSlotAllocationSucceeds() throws Exception {
        testSlotAllocation((ignored0, ignored1, ignored2, ignored3) -> {});
    }

    @Test
    void testAllocationUpdatesIgnoredIfSlotFreed() throws Exception {
        testSlotAllocation(
                (slotStatusSyncer, taskManagerTracker, ignored, allocationId) -> {
                    slotStatusSyncer.freeSlot(allocationId);
                    assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId))
                            .isEmpty();
                });
    }

    @Test
    void testAllocateSlotFailsWithException() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                ignored ->
                                        FutureUtils.completedExceptionally(
                                                new TimeoutException("timeout")))
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        taskManagerTracker.addTaskManager(
                taskExecutorConnection, ResourceProfile.ANY, ResourceProfile.ANY);
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final JobID jobId = new JobID();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                EXECUTOR_RESOURCE.getExecutor());

        final CompletableFuture<Void> allocatedFuture =
                slotStatusSyncer.allocateSlot(
                        taskExecutorConnection.getInstanceID(),
                        jobId,
                        "address",
                        ResourceProfile.ANY);

        assertThatThrownBy(allocatedFuture::get).hasCauseInstanceOf(TimeoutException.class);
        assertThat(resourceTracker.getAcquiredResources(jobId)).isEmpty();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                taskExecutorConnection.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAllocatedSlots()).isEmpty());
    }

    @Test
    void testAllocationUpdatesIgnoredIfSlotRemoved() throws Exception {
        testSlotAllocation(
                (slotStatusSyncer, taskManagerTracker, instanceID, allocationId) -> {
                    taskManagerTracker.removeTaskManager(instanceID);
                    assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId))
                            .isEmpty();
                });
    }

    @Test
    void testFreeSlot() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                EXECUTOR_RESOURCE.getExecutor());
        taskManagerTracker.addTaskManager(
                TASK_EXECUTOR_CONNECTION, ResourceProfile.ANY, ResourceProfile.ANY);
        taskManagerTracker.notifySlotStatus(
                allocationId,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.ANY,
                SlotState.ALLOCATED);
        resourceTracker.notifyAcquiredResource(jobId, ResourceProfile.ANY);

        // unknown slot will be ignored.
        slotStatusSyncer.freeSlot(new AllocationID());
        assertThat(resourceTracker.getAcquiredResources(jobId))
                .containsExactly(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId)).isPresent();

        slotStatusSyncer.freeSlot(allocationId);
        assertThat(resourceTracker.getAcquiredResources(jobId)).isEmpty();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAllocatedSlots()).isEmpty());
    }

    @Test
    void testSlotStatusProcessing() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                EXECUTOR_RESOURCE.getExecutor());
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(ignored -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final JobID jobId = new JobID();
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final SlotID slotId1 = new SlotID(taskExecutorConnection.getResourceID(), 0);
        final SlotID slotId2 = new SlotID(taskExecutorConnection.getResourceID(), 1);
        final SlotID slotId3 = new SlotID(taskExecutorConnection.getResourceID(), 2);
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        final SlotReport slotReport1 =
                new SlotReport(
                        Arrays.asList(
                                new SlotStatus(slotId1, totalResource),
                                new SlotStatus(slotId2, resource, jobId, allocationId1),
                                new SlotStatus(slotId3, resource, jobId, allocationId2)));
        final SlotReport slotReport2 =
                new SlotReport(
                        Arrays.asList(
                                new SlotStatus(slotId3, resource),
                                new SlotStatus(slotId2, resource, jobId, allocationId1)));
        taskManagerTracker.addTaskManager(taskExecutorConnection, totalResource, totalResource);

        slotStatusSyncer.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport1);
        assertThat(resourceTracker.getAcquiredResources(jobId))
                .contains(ResourceRequirement.create(resource, 2));
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                taskExecutorConnection.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(3, 12)));
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1)).isPresent();
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2)).isPresent();

        slotStatusSyncer.allocateSlot(
                taskExecutorConnection.getInstanceID(), jobId, "address", resource);
        assertThat(resourceTracker.getAcquiredResources(jobId))
                .contains(ResourceRequirement.create(resource, 3));
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                taskExecutorConnection.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(2, 8)));
        final AllocationID allocationId3 =
                taskManagerTracker
                        .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                        .get()
                        .getAllocatedSlots()
                        .keySet()
                        .stream()
                        .filter(
                                allocationId ->
                                        !allocationId.equals(allocationId1)
                                                && !allocationId.equals(allocationId2))
                        .findAny()
                        .get();

        // allocationId1 should still be allocated; allocationId2 should be freed; allocationId3
        // should continue to be in a pending state;
        slotStatusSyncer.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport2);
        assertThat(resourceTracker.getAcquiredResources(jobId))
                .contains(ResourceRequirement.create(resource, 2));
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                taskExecutorConnection.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(3, 12)));
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2)).isNotPresent();
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1))
                .hasValueSatisfying(
                        slot -> assertThat(slot.getState()).isEqualTo(SlotState.ALLOCATED));
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId3))
                .hasValueSatisfying(
                        slot -> assertThat(slot.getState()).isEqualTo(SlotState.PENDING));
    }

    private static void testSlotAllocation(
            QuadConsumer<SlotStatusSyncer, TaskManagerTracker, InstanceID, AllocationID>
                    beforeCompletingSlotRequestCallback)
            throws ExecutionException, InterruptedException {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final CompletableFuture<AllocationID> requestFuture = new CompletableFuture<>();

        final CompletableFuture<Acknowledge> responseFuture = new CompletableFuture<>();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestFuture.complete(tuple6.f2);
                                    return responseFuture;
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        taskManagerTracker.addTaskManager(
                taskExecutorConnection, ResourceProfile.ANY, ResourceProfile.ANY);
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final JobID jobId = new JobID();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                EXECUTOR_RESOURCE.getExecutor());

        final CompletableFuture<Void> allocatedFuture =
                slotStatusSyncer.allocateSlot(
                        taskExecutorConnection.getInstanceID(),
                        jobId,
                        "address",
                        ResourceProfile.ANY);
        final AllocationID allocationId = requestFuture.get();
        assertThat(resourceTracker.getAcquiredResources(jobId))
                .contains(ResourceRequirement.create(ResourceProfile.ANY, 1));
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId))
                .hasValueSatisfying(
                        slot -> {
                            assertThat(slot.getJobId()).isEqualTo(jobId);
                            assertThat(slot.getState()).isEqualTo(SlotState.PENDING);
                        });

        beforeCompletingSlotRequestCallback.accept(
                slotStatusSyncer,
                taskManagerTracker,
                taskExecutorConnection.getInstanceID(),
                allocationId);

        responseFuture.complete(Acknowledge.get());
        assertThatFuture(allocatedFuture).eventuallySucceeds();
    }
}
