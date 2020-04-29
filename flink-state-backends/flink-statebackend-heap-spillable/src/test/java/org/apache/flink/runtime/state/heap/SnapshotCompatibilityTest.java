/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * TODO This test is just to verify the snapshot format, and should be removed finllay.
 * Because spillable state backend may have it's own snapshot format.
 */
public class SnapshotCompatibilityTest {

	private final TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void checkCompatibleSerializationFormats() throws IOException {
		final Random r = new Random(42);
		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.VALUE,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE));

		final MockInternalKeyContext<Integer> keyContext = new MockInternalKeyContext<>();

		CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> cowStateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo, keySerializer);

		for (int i = 0; i < 100; ++i) {
			ArrayList<Integer> list = new ArrayList<>(5);
			int end = r.nextInt(5);
			for (int j = 0; j < end; ++j) {
				list.add(r.nextInt(100));
			}

			keyContext.setCurrentKey(r.nextInt(10));
			cowStateTable.put(r.nextInt(2), list);
		}

		StateSnapshot snapshot = cowStateTable.stateSnapshot();

		Configuration configuration = new Configuration();
		configuration.set(SpillableOptions.SPACE_TYPE, SpaceAllocator.SpaceType.MMAP.name());
		SpaceAllocator spaceAllocator = new SpaceAllocator(configuration, new File[] {tmp.newFolder()});
		SpillAndLoadManagerImpl spillAndLoadManager = new SpillAndLoadManagerImpl(
			new SpillAndLoadManagerImpl.StateTableContainerImpl<>(new HashMap<>()),
			new TestHeapStatusMonitor(Long.MAX_VALUE), new Configuration());
		final SpillableStateTableImpl<Integer, Integer, ArrayList<Integer>> nestedMapsStateTable =
			new SpillableStateTableImpl<>(keyContext, metaInfo, keySerializer, spaceAllocator, spillAndLoadManager);

		restoreStateTableFromSnapshot(nestedMapsStateTable, snapshot, keyContext.getKeyGroupRange());
		snapshot.release();

		Assert.assertEquals(cowStateTable.size(), nestedMapsStateTable.size());
		for (StateEntry<Integer, Integer, ArrayList<Integer>> entry : cowStateTable) {
			Assert.assertEquals(entry.getState(), nestedMapsStateTable.get(entry.getKey(), entry.getNamespace()));
		}

		snapshot = nestedMapsStateTable.stateSnapshot();
		cowStateTable = new CopyOnWriteStateTable<>(keyContext, metaInfo, keySerializer);

		restoreStateTableFromSnapshot(cowStateTable, snapshot, keyContext.getKeyGroupRange());
		snapshot.release();

		Assert.assertEquals(nestedMapsStateTable.size(), cowStateTable.size());
		for (StateEntry<Integer, Integer, ArrayList<Integer>> entry : cowStateTable) {
			Assert.assertEquals(nestedMapsStateTable.get(entry.getKey(), entry.getNamespace()), entry.getState());
		}
	}

	private void restoreStateTableFromSnapshot(
		StateTable<Integer, Integer, ArrayList<Integer>> stateTable,
		StateSnapshot snapshot,
		KeyGroupRange keyGroupRange) throws IOException {

		final ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos(1024 * 1024);
		final DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(out);
		final StateSnapshot.StateKeyGroupWriter keyGroupPartitionedSnapshot = snapshot.getKeyGroupWriter();
		for (Integer keyGroup : keyGroupRange) {
			keyGroupPartitionedSnapshot.writeStateInKeyGroup(dov, keyGroup);
		}

		final ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(out.getBuf());
		final DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(in);

		final StateSnapshotKeyGroupReader keyGroupReader =
			StateTableByKeyGroupReaders.readerForVersion(stateTable, KeyedBackendSerializationProxy.VERSION);

		for (Integer keyGroup : keyGroupRange) {
			keyGroupReader.readMappingsInKeyGroup(div, keyGroup);
		}
	}
}
