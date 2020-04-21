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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.estimate.SampleStateMemoryEstimator;
import org.apache.flink.runtime.state.heap.estimate.StateMemoryEstimatorFactory;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Spillable state table.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> tyep of state
 */
public class SpillableStateTable<K, N, S> extends StateTable<K, N, S> implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(SpillableStateTable.class);

	private static final int DEFAULT_ESTIMATE_SAMPLE_COUNT = 1000;

	private static final int DEFAULT_NUM_KEYS_TO_DELETED_ONE_TIME = 2;

	private static final float DEFAULT_LOGICAL_REMOVE_KEYS_RATIO = 0.2f;

	private final SpaceAllocator spaceAllocator;

	private final SpillAndLoadManager spillAndLoadManager;

	private final SampleStateMemoryEstimator<K, N, S> stateMemoryEstimator;

	private final StateTransformationFunctionWrapper transformationWrapper;

	/** Number of requests for each key group. */
	private final long[] numRequests;

	SpillableStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer,
		SpaceAllocator spaceAllocator,
		SpillAndLoadManager spillAndLoadManager) {
		this(keyContext, metaInfo, keySerializer, spaceAllocator, spillAndLoadManager, DEFAULT_ESTIMATE_SAMPLE_COUNT);
	}

	/**
	 * Constructs a new {@code SpillableStateTable}.
	 *
	 * @param keyContext    the key context.
	 * @param metaInfo      the meta information, including the type serializer for state copy-on-write.
	 * @param keySerializer the serializer of the key.
	 * @param spaceAllocator the space allocator.
	 * @param estimateSampleCount sample numRequests to estimate state memory.
	 */
	SpillableStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer,
		SpaceAllocator spaceAllocator,
		SpillAndLoadManager spillAndLoadManager,
		int estimateSampleCount) {
		super(keyContext, metaInfo, keySerializer);
		this.spaceAllocator = spaceAllocator;
		this.spillAndLoadManager = spillAndLoadManager;
		for (int i = 0; i < this.keyGroupedStateMaps.length; i++) {
			if (keyGroupedStateMaps[i] == null) {
				this.keyGroupedStateMaps[i] = createStateMap();
			}
		}
		this.stateMemoryEstimator = StateMemoryEstimatorFactory.createSampleEstimator(keySerializer, metaInfo, estimateSampleCount);
		this.transformationWrapper = new StateTransformationFunctionWrapper();
		this.numRequests = new long[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
		for (int i = 0; i < numRequests.length; i++) {
			numRequests[i] = 0;
		}
	}

	@Override
	protected StateMap<K, N, S> createStateMap() {
		return createHashStateMap();
	}

	private CopyOnWriteStateMap<K, N, S> createHashStateMap() {
		return new CopyOnWriteStateMap<>(getStateSerializer());
	}

	private CopyOnWriteSkipListStateMap<K, N, S> createSkipListStateMap() {
		return new CopyOnWriteSkipListStateMap<>(getKeySerializer(),
			getNamespaceSerializer(),
			getStateSerializer(),
			spaceAllocator,
			DEFAULT_NUM_KEYS_TO_DELETED_ONE_TIME,
			DEFAULT_LOGICAL_REMOVE_KEYS_RATIO);
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	/**
	 * Creates a snapshot of this {@link SpillableStateTable}, to be written in checkpointing.
	 *
	 * @return a snapshot from this {@link SpillableStateTable}, for checkpointing.
	 */
	@Nonnull
	@Override
	public SpillableStateTableSnapshot<K, N, S> stateSnapshot() {
		return new SpillableStateTableSnapshot<>(
			this,
			getKeySerializer().duplicate(),
			getNamespaceSerializer().duplicate(),
			getStateSerializer().duplicate(),
			getMetaInfo().getStateSnapshotTransformFactory().createForDeserializedState().orElse(null));
	}

	@SuppressWarnings("unchecked")
	List<StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>>> getStateMapSnapshotList() {
		List<StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>>> snapshotList = new ArrayList<>(keyGroupedStateMaps.length);
		for (int i = 0; i < keyGroupedStateMaps.length; i++) {
			snapshotList.add(keyGroupedStateMaps[i].stateSnapshot());
		}
		return snapshotList;
	}

	@Override
	public void close() {
		// TODO release resource
	}

	public InternalKeyContext<K> getInternalKeyContext() {
		return keyContext;
	}

	/**
	 * Return the estimated size of state. When {@param force} is true,
	 * an estimation will be made if there hasn't been an estimation.
	 */
	public long getStateEstimatedSize(boolean force) {
		long estimatedSize = stateMemoryEstimator.getEstimatedSize();
		if (estimatedSize == -1 && force) {
			for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
				if (stateMap.isEmpty()) {
					continue;
				}
				StateEntry<K, N, S> stateEntry = stateMap.iterator().next();
				stateMemoryEstimator.forceUpdateEstimatedSize(
					stateEntry.getKey(), stateEntry.getNamespace(), stateEntry.getState());
			}
			estimatedSize = stateMemoryEstimator.getEstimatedSize();
		}

		return estimatedSize;
	}

	public void updateStateEstimate(N namespace, S state) {
		stateMemoryEstimator.updateEstimatedSize(keyContext.getCurrentKey(), namespace, state);
	}

	public StateMap<K, N, S> getCurrentStateMap() {
		return getMapForKeyGroup(keyContext.getCurrentKeyGroupIndex());
	}

	@Override
	public void put(N namespace, S state) {
		super.put(namespace, state);
		updateStateEstimate(namespace, state);
		spillAndLoadManager.checkResource();
	}

	@Override
	public void put(K key, int keyGroup, N namespace, S state) {
		super.put(key, keyGroup, namespace, state);
		stateMemoryEstimator.updateEstimatedSize(key, namespace, state);
		spillAndLoadManager.checkResource();
	}

	@Override
	public <T> void transform(
		N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		transformationWrapper.setStateTransformationFunction(namespace, transformation);
		super.transform(namespace, value, transformationWrapper);
		spillAndLoadManager.checkResource();
	}

	class StateTransformationFunctionWrapper<T> implements StateTransformationFunction<S, T> {

		private N namespace;
		private StateTransformationFunction<S, T> stateTransformationFunction;

		public StateTransformationFunction<S, T> getStateTransformationFunction() {
			return stateTransformationFunction;
		}

		public void setStateTransformationFunction(
			N namespace,
			StateTransformationFunction<S, T> stateTransformationFunction) {
			this.namespace = namespace;
			this.stateTransformationFunction = stateTransformationFunction;
		}

		@Override
		public S apply(S previousState, T value) throws Exception {
			S newState = stateTransformationFunction.apply(previousState, value);
			if (newState != null) {
				updateStateEstimate(namespace, newState);
			}

			return newState;
		}
	}

	public void spillState(int keyGroupIndex) {
		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);
		Preconditions.checkState(stateMap instanceof CopyOnWriteStateMap,
			"Only CopyOnWriteStateMap can be spilled");
		CopyOnWriteSkipListStateMap<K, N, S> dstStatMap = createSkipListStateMap();
		try {
			transferState(stateMap, dstStatMap);
		} catch (Exception e) {
			LOG.error("Spill state in keygroup {} failed", keyGroupIndex, e);
			IOUtils.closeQuietly(dstStatMap);
			throw e;
		}

		setMapForKeyGroup(keyGroupIndex, dstStatMap);
	}

	public void loadState(int keyGroupIndex) {
		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);
		Preconditions.checkState(stateMap instanceof CopyOnWriteSkipListStateMap,
			"Only CopyOnWriteSkipListStateMap can be loaded");

		CopyOnWriteStateMap<K, N, S> dstStatMap = createHashStateMap();
		try {
			transferState(stateMap, dstStatMap);
		} catch (Exception e) {
			LOG.error("Load state in keygroup {} failed", keyGroupIndex, e);
			throw e;
		}

		setMapForKeyGroup(keyGroupIndex, dstStatMap);

		try {
			((CopyOnWriteSkipListStateMap) stateMap).close();
		} catch (Exception e) {
			LOG.error("Failed to close state map for keygroup {} after load", keyGroupIndex, e);
			throw e;
		}
	}

	private void transferState(StateMap<K, N, S> srcStateMap, StateMap<K, N, S> dstStateMap) {
		for (StateEntry<K, N, S> entry : srcStateMap) {
			dstStateMap.put(entry.getKey(), entry.getNamespace(), entry.getState());
		}
	}

	@Override
	StateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
		final int pos = indexToOffset(keyGroupIndex);
		if (pos >= 0 && pos < keyGroupedStateMaps.length) {
			numRequests[pos] = numRequests[pos] + 1;
			return keyGroupedStateMaps[pos];
		} else {
			return null;
		}
	}

	private void setMapForKeyGroup(int keyGroupIndex, StateMap<K, N, S> stateMap) {
		final int pos = indexToOffset(keyGroupIndex);
		keyGroupedStateMaps[pos] = stateMap;
	}

	public Iterator<StateMapMeta> stateMapIterator() {
		return new Iterator<StateMapMeta>() {
			int next = 0;

			@Override
			public boolean hasNext() {
				return next < keyGroupedStateMaps.length;
			}

			@Override
			public StateMapMeta next() {
				StateMapMeta stateMapMeta = new StateMapMeta(
					SpillableStateTable.this,
					keyGroupedStateMaps[next],
					next + keyGroupOffset,
					numRequests[next]);
				next++;
				return stateMapMeta;
			}
		};
	}

	/**
	 * Meta of a {@link StateMap}.
	 */
	public static class StateMapMeta {

		private final SpillableStateTable stateTable;
		private final StateMap stateMap;
		private final int keyGroupIndex;
		private final long numRequests;
		/** Initialize lazily. -1 indicates uninitialized. */
		private long estimatedMemorySize;

		public StateMapMeta(
			SpillableStateTable stateTable,
			StateMap stateMap,
			int keyGroupIndex,
			long count) {
			this.stateTable = stateTable;
			this.stateMap = stateMap;
			this.keyGroupIndex = keyGroupIndex;
			this.numRequests = count;
			this.estimatedMemorySize = -1;
		}

		public SpillableStateTable getStateTable() {
			return stateTable;
		}

		public StateMap getStateMap() {
			return stateMap;
		}

		public boolean isOnHeap() {
			return stateMap instanceof CopyOnWriteStateMap;
		}

		public int getSize() {
			return stateMap.size();
		}

		public int getKeyGroupIndex() {
			return keyGroupIndex;
		}

		public long getNumRequests() {
			return numRequests;
		}

		public long getEstimatedMemorySize() {
			return estimatedMemorySize;
		}

		public void setEstimatedMemorySize(long estimatedMemorySize) {
			this.estimatedMemorySize = estimatedMemorySize;
		}
	}
}
