/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.types.Row;
import org.apache.flink.util.ResourceGuard;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_VALUE_POINTER;

/**
 * This class represents the snapshot of a {@link CopyOnWriteSkipListStateMap}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class CopyOnWriteSkipListStateMapSnapshot<K, N, S>
	extends StateMapSnapshot<K, N, S, CopyOnWriteSkipListStateMap<K, N, S>> {

	/**
	 * Version of the {@link CopyOnWriteSkipListStateMap} when this snapshot was created. This can be used to release the snapshot.
	 */
	private final int snapshotVersion;

	/** The number of (non-null) entries in snapshotData. */
	@Nonnegative
	private final int numberOfEntriesInSnapshotData;

	/**
	 * Header of level index.
	 */
	private final LevelIndexHeader levelIndexHeader;

	/**
	 * This lease protects the state map resources.
	 */
	private final ResourceGuard.Lease lease;

	/**
	 * Creates a new {@link CopyOnWriteSkipListStateMap}.
	 *
	 * @param owningStateMap the {@link CopyOnWriteSkipListStateMap} for which this object represents a snapshot.
	 * @param lease the lease protects the state map resources.
	 */
	CopyOnWriteSkipListStateMapSnapshot(
		CopyOnWriteSkipListStateMap<K, N, S> owningStateMap,
		ResourceGuard.Lease lease) {
		super(owningStateMap);

		this.snapshotVersion = owningStateMap.getStateMapVersion();
		this.numberOfEntriesInSnapshotData = owningStateMap.size();
		this.levelIndexHeader = owningStateMap.getLevelIndexHeader();
		this.lease = lease;
	}

	/**
	 * Returns the internal version of the when this snapshot was created.
	 */
	int getSnapshotVersion() {
		return snapshotVersion;
	}

	@Override
	public void release() {
		owningStateMap.releaseSnapshot(this);
		lease.close();
	}

	@Override
	public void writeState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer,
		@Nonnull DataOutputView dov,
		@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) throws IOException {
		int size = stateSnapshotTransformer == null ? numberOfEntriesInSnapshotData :
			getSizeAfterTransformed(stateSnapshotTransformer);

		// TODO I will finish this part this week (before 7.28)
		// 1. copy key and namespace from bytes rather than deserialize/serialize
		// 2. when transformer is null, we can scan the map only once
	}

	private int getSizeAfterTransformed(
		StateSnapshotTransformer<S> stateSnapshotTransformer,
		CopyOnWriteSkipListStateMap.SkipListValueSerializer<S> serializer) {
		SnapshotNodeIterator nodeIterator = new SnapshotNodeIterator();
		int size = 0;
		while (nodeIterator.hasNext()) {
			Tuple2<Long, Long> tuple = nodeIterator.next();
			// TODO concurrent state serializer
			S oldState = owningStateMap.helpGetState(tuple.f1, serializer);
			S newState = stateSnapshotTransformer.filterOrTransform(oldState);
			if (newState != null) {
				size++;
			}
		}

		return size;
	}

	/**
	 * Iterates over all nodes used by this snapshot. The iterator will return
	 * a tuple, and f0 is the node and f1 is the value pointer.
	 */
	class SnapshotNodeIterator implements Iterator<Tuple2<Long, Long>> {

		private long nextNode;
		private long nextValuePointer;

		SnapshotNodeIterator() {
			this.nextNode = levelIndexHeader.getNextNode(0);
			advance();
		}

		private void advance() {
			if (nextNode == NIL_NODE) {
				return;
			}

			long node = owningStateMap.helpGetNextNode(nextNode, 0);
			long valuePointer = NIL_VALUE_POINTER;
			while (node != NIL_NODE) {
				valuePointer = owningStateMap.getAndPruneValue(node, snapshotVersion);
				int valueLen = valuePointer == NIL_VALUE_POINTER ? 0 :
					owningStateMap.helpGetValueLen(valuePointer);
				if (valueLen != 0) {
					break;
				}
				node = owningStateMap.helpGetNextNode(node, 0);
			}

			nextNode = node;
			nextValuePointer = valuePointer;
		}

		@Override
		public boolean hasNext() {
			return nextNode != NIL_NODE;
		}

		@Override
		public Tuple2<Long, Long> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			long node = nextNode;
			long valuePointer = nextValuePointer;
			advance();

			return Tuple2.of(node, valuePointer);
		}
	}
}
