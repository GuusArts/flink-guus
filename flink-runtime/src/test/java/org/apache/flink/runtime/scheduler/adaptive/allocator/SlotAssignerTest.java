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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAssigner.getSlotsPerTaskExecutor;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.StateLocalitySlotAssigner.AllocationScore;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SlotAssigner}. */
class SlotAssignerTest {

    private static final TaskManagerLocation tml1 = new LocalTaskManagerLocation();
    private static final SlotInfo slot1OfTml1 = new TestingSlot(tml1);
    private static final SlotInfo slot2OfTml1 = new TestingSlot(tml1);
    private static final SlotInfo slot3OfTml1 = new TestingSlot(tml1);

    private static final TaskManagerLocation tml2 = new LocalTaskManagerLocation();
    private static final SlotInfo slot1OfTml2 = new TestingSlot(tml2);
    private static final SlotInfo slot2OfTml2 = new TestingSlot(tml2);
    private static final SlotInfo slot3OfTml2 = new TestingSlot(tml2);

    private static final TaskManagerLocation tml3 = new LocalTaskManagerLocation();
    private static final SlotInfo slot1OfTml3 = new TestingSlot(tml3);
    private static final SlotInfo slot2OfTml3 = new TestingSlot(tml3);

    private static final List<SlotInfo> allSlots =
            Arrays.asList(
                    slot1OfTml1,
                    slot2OfTml1,
                    slot3OfTml1,
                    slot1OfTml2,
                    slot2OfTml2,
                    slot3OfTml2,
                    slot1OfTml3,
                    slot2OfTml3);

    private static Stream<Arguments> getTestingParameters() {
        return Stream.of(
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        3,
                        allSlots,
                        createTestingScores(Tuple2.of(slot1OfTml1, 2L), Tuple2.of(slot1OfTml2, 1L)),
                        Arrays.asList(tml1, tml3)),
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        2,
                        allSlots,
                        createTestingScores(Tuple2.of(slot1OfTml1, 2L), Tuple2.of(slot1OfTml2, 2L)),
                        Collections.singletonList(tml3)),
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        6,
                        allSlots,
                        createTestingScores(Tuple2.of(slot1OfTml2, 2L)),
                        Arrays.asList(tml1, tml2, tml3)),
                Arguments.of(
                        new StateLocalitySlotAssigner(),
                        4,
                        Arrays.asList(
                                slot1OfTml1,
                                slot2OfTml1,
                                slot1OfTml2,
                                slot2OfTml2,
                                slot1OfTml3,
                                slot2OfTml3),
                        createTestingScores(Tuple2.of(slot1OfTml2, 2L), Tuple2.of(slot2OfTml3, 1L)),
                        Arrays.asList(tml2, tml3)),
                Arguments.of(
                        new DefaultSlotAssigner(),
                        2,
                        allSlots,
                        null,
                        Collections.singletonList(tml3)),
                Arguments.of(
                        new DefaultSlotAssigner(),
                        3,
                        Arrays.asList(slot1OfTml1, slot1OfTml2, slot2OfTml2, slot3OfTml2),
                        null,
                        Arrays.asList(tml1, tml2)),
                Arguments.of(
                        new DefaultSlotAssigner(),
                        7,
                        allSlots,
                        createTestingScores(Tuple2.of(slot1OfTml2, 2L)),
                        Arrays.asList(tml1, tml2, tml3)));
    }

    @MethodSource("getTestingParameters")
    @ParameterizedTest(
            name =
                    "slotAssigner={0}, groups={1}, allSlots={2}, scoredAllocations={3}, minimalTaskExecutors={4}")
    void testSelectSlotsInMinimalTaskExecutors(
            SlotAssigner slotAssigner,
            int requestGroups,
            List<SlotInfo> allSlots,
            PriorityQueue<AllocationScore> scores,
            List<TaskManagerLocation> minimalTaskExecutors) {
        Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor =
                getSlotsPerTaskExecutor(allSlots);
        List<TaskManagerLocation> sortedTaskExecutors;
        if (slotAssigner instanceof StateLocalitySlotAssigner) {
            sortedTaskExecutors =
                    ((StateLocalitySlotAssigner) slotAssigner)
                            .getSortedTaskExecutors(allSlots, slotsPerTaskExecutor, scores);
        } else if (slotAssigner instanceof DefaultSlotAssigner) {
            sortedTaskExecutors =
                    ((DefaultSlotAssigner) slotAssigner)
                            .getSortedTaskExecutors(slotsPerTaskExecutor);
        } else {
            throw new IllegalStateException(
                    "Unexpected implementations of " + SlotAssigner.class.getName());
        }

        Set<TaskManagerLocation> keptTaskExecutors =
                slotAssigner
                        .selectSlotsInMinimalTaskExecutors(
                                allSlots, slotsPerTaskExecutor, requestGroups, sortedTaskExecutors)
                        .stream()
                        .map(SlotInfo::getTaskManagerLocation)
                        .collect(Collectors.toSet());
        assertThat(minimalTaskExecutors).containsAll(keptTaskExecutors);
    }

    @SafeVarargs
    private static PriorityQueue<AllocationScore> createTestingScores(
            Tuple2<SlotInfo, Long>... scorePairs) {
        final PriorityQueue<AllocationScore> scores =
                new PriorityQueue<>(Comparator.reverseOrder());
        Arrays.stream(scorePairs)
                .map(t2 -> new AllocationScore("unUsedGid", t2.f0.getAllocationId(), t2.f1))
                .forEach(scores::add);
        return scores;
    }
}
