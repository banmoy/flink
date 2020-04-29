/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;
import org.apache.flink.util.Preconditions;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SpillableStateTableImpl}.
 */
public class SpillableStateTableTest {

	private final TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
	private final RegisteredKeyValueStateBackendMetaInfo<Integer, Integer> metaInfo =
		new RegisteredKeyValueStateBackendMetaInfo<>(
			StateDescriptor.Type.VALUE, "test",
			IntSerializer.INSTANCE, IntSerializer.INSTANCE);
	private final int numberOfKeyGroups = 5;
	private final MockInternalKeyContext<Integer> keyContext =
		new MockInternalKeyContext<>(0, numberOfKeyGroups - 1, numberOfKeyGroups);

	private SpaceAllocator spaceAllocator;
	private SpillableStateTableImpl<Integer, Integer, Integer> stateTable;

	@Before
	public void setUp() {
		Configuration conf = new Configuration();
		conf.set(SpillableOptions.SPACE_TYPE, SpaceAllocator.SpaceType.HEAP.name());
		conf.set(SpillableOptions.CHUNK_SIZE, MemorySize.ofMebiBytes(64));
		this.spaceAllocator = new SpaceAllocator(conf, null);
		this.stateTable = new SpillableStateTableImpl<>(
			keyContext, metaInfo, keySerializer, spaceAllocator, () -> {});
	}

	@After
	public void tearDown() throws Exception {
		stateTable.close();
		spaceAllocator.close();
	}

	@Test
	public void testSpill() {
		Map<Integer, Tuple2<Integer, Integer>> expectedData = genData(0, 10000);

		storeData(expectedData);
		verifyData(expectedData);

		// verify that all states are on-heap
		for (int i = 0; i < numberOfKeyGroups; i++) {
			assertTrue(stateTable.getMapForKeyGroup(i) instanceof CopyOnWriteStateMap);
		}

		// spill states by key group
		for (int i = 0; i < numberOfKeyGroups; i++) {
			stateTable.spillState(i);
			assertTrue(stateTable.getMapForKeyGroup(i) instanceof CopyOnWriteSkipListStateMap);

			// add some new state
			storeData(genData(10000 + i * 1000, 10000 + (i + 1) * 1000));
			verifyData(expectedData);
		}
	}

	@Test
	public void testLoad() {
		Map<Integer, Tuple2<Integer, Integer>> expectedData = genData(0, 10000);

		storeData(expectedData);

		// spill all states first
		for (int i = 0; i < numberOfKeyGroups; i++) {
			stateTable.spillState(i);
		}

		// verify that all states are off-heap
		for (int i = 0; i < numberOfKeyGroups; i++) {
			assertTrue(stateTable.getMapForKeyGroup(i) instanceof CopyOnWriteSkipListStateMap);
		}
		verifyData(expectedData);

		// load states by key group
		for (int i = 0; i < numberOfKeyGroups; i++) {
			stateTable.loadState(i);
			assertTrue(stateTable.getMapForKeyGroup(i) instanceof CopyOnWriteStateMap);

			// add some new state
			storeData(genData(10000 + i * 1000, 10000 + (i + 1) * 1000));
			verifyData(expectedData);
		}
	}

	@Test
	public void testSnapshotWhenSpill() {

	}

	@Test
	public void testSnapshotWhenLoad() {

	}

	private void storeData(Map<Integer, Tuple2<Integer, Integer>> data) {
		for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : data.entrySet()) {
			keyContext.setCurrentKeyAndKeyGroup(entry.getKey());
			stateTable.put(entry.getValue().f0, entry.getValue().f1);
		}
	}

	private Map<Integer, Tuple2<Integer, Integer>> genData(int startKeyInclusive, int endKeyExclusive) {
		Preconditions.checkArgument(startKeyInclusive < endKeyExclusive);
		ThreadLocalRandom random = ThreadLocalRandom.current();
		Map<Integer, Tuple2<Integer, Integer>> data = new HashMap<>();
		for (int i = startKeyInclusive; i < endKeyExclusive; i++) {
			data.put(i, Tuple2.of(random.nextInt(), random.nextInt()));
		}

		return data;
	}

	private void verifyData(Map<Integer, Tuple2<Integer, Integer>> expectedData) {
		for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : expectedData.entrySet()) {
			keyContext.setCurrentKeyAndKeyGroup(entry.getKey());
			assertEquals(entry.getValue().f1, stateTable.get(entry.getValue().f0));
		}
	}
}
