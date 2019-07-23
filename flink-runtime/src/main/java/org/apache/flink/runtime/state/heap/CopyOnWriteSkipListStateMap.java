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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.space.Chunk;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;
import org.apache.flink.runtime.state.heap.space.SpaceUtils;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.state.heap.SkipListUtils.HEAD_NODE;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_VALUE_POINTER;

/**
 * Implementation of state map which uses skip list data structure with copy-on-write support.
 * The states will be serialized to bytes and stored in the space allocated with the given allocator.
 */
public class CopyOnWriteSkipListStateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteSkipListStateMap.class);

	/**
	 * Max number of logically-removed keys to delete one time.
	 */
	private static final int MAX_KEYS_TO_DELETE_ONE_TIME = 2;

	/**
	 * Ratio of the logically-removed keys to trigger deletion when snapshot.
	 */
	private static final float LOGICAL_REMOVED_KEYS_RATIO = 0.2f;

	/**
	 * The serializer of the state.
	 */
	protected final TypeSerializer<S> stateSerializer;

	/**
	 * The serializer used to serialize the key and namespace to bytes stored in skip list.
	 */
	private final SkipListKeySerializer<K, N> skipListKeySerializer;

	/**
	 * The serializer used to serialize the state to bytes stored in skip list.
	 */
	private final SkipListValueSerializer<S> skipListValueSerializer;

	/**
	 * The comparator used to compare keys in skip list.
	 */
	private final SkipListKeyComparator skipListKeyComparator;

	/**
	 * Space allocator.
	 */
	private final SpaceAllocator spaceAllocator;

	/**
	 * The level index header.
	 */
	private final LevelIndexHeader levelIndexHeader;

	/**
	 * Seed to generate random index level.
	 */
	private int randomSeed;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 */
	private int stateMapVersion;

	/**
	 * The highest version of this map that is still required by any unreleased snapshot.
	 */
	private int highestRequiredSnapshotVersion;

	/**
	 * Snapshots no more than this version must have been finished, but there may be some
	 * snapshots more than this version are still running.
	 */
	private volatile int highestFinishedSnapshotVersion;

	/**
	 * Maintains an ordered set of version ids that are still used by unreleased snapshots.
	 */
	private final TreeSet<Integer> snapshotVersions;

	/**
	 * The size of skip list which includes the logical removed keys.
	 */
	private int totalSize;

	/**
	 * Number of requests for this skip list.
	 */
	private int requestCount;

	/**
	 * Set of logical removed nodes.
	 */
	private final Set<Long> logicallyRemovedNodes;

	/**
	 * Number of keys to remove physically one time.
	 */
	private final int numKeysToDeleteOneTime;

	/**
	 * Set of nodes whose values are being pruned by snapshots.
	 */
	private final Set<Long> pruningValueNodes;

	/**
	 * Whether this map has been closed.
	 */
	private final AtomicBoolean closed;

	/**
	 * Guards for the free of space when state map is closed. This is mainly
	 * used to synchronize with snapshots.
	 */
	private final ResourceGuard resourceGuard;

	public CopyOnWriteSkipListStateMap(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer,
		SpaceAllocator spaceAllocator) {
		this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
		this.skipListKeySerializer = new SkipListKeySerializer<>(keySerializer, namespaceSerializer);
		this.skipListValueSerializer = new SkipListValueSerializer<>(stateSerializer);
		this.skipListKeyComparator = new SkipListKeyComparator();
		this.numKeysToDeleteOneTime = MAX_KEYS_TO_DELETE_ONE_TIME;
		this.spaceAllocator = Preconditions.checkNotNull(spaceAllocator);

		this.levelIndexHeader = new OnHeapLevelIndexHeader();
		this.randomSeed = ThreadLocalRandom.current().nextInt() | 0x0100;

		this.stateMapVersion = 0;
		this.highestRequiredSnapshotVersion = 0;
		this.highestFinishedSnapshotVersion = 0;
		this.snapshotVersions = new TreeSet<>();

		this.totalSize = 0;
		this.requestCount = 0;

		this.logicallyRemovedNodes = new HashSet<>();
		this.pruningValueNodes = ConcurrentHashMap.newKeySet();

		this.closed = new AtomicBoolean(false);
		this.resourceGuard = new ResourceGuard();
	}

	@Override
	public int size() {
		return totalSize - logicallyRemovedNodes.size();
	}

	public int totalSize() {
		return totalSize;
	}

	public int getRequestCount() {
		return requestCount;
	}

	@Override
	public S get(K key, N namespace) {
		updateStat();
		byte[] keyBytes =skipListKeySerializer.serialize(key, namespace);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
		int keyLen = keyBytes.length;

		long node = getNode(keyByteBuffer, 0, keyLen);

		if (node == NIL_NODE) {
			return null;
		}

		return helpGetNodeState(node);
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		updateStat();
		byte[] keyBytes =skipListKeySerializer.serialize(key, namespace);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
		int keyLen = keyBytes.length;

		long node = getNode(keyByteBuffer, 0, keyLen);

		return node != NIL_NODE;
	}

	@Override
	public void put(K key, N namespace, S state) {
		updateStat();
		byte[] keyBytes =skipListKeySerializer.serialize(key, namespace);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
		int keyLen = keyBytes.length;
		byte[] value = skipListValueSerializer.serialize(state);

		putNode(keyByteBuffer, 0, keyLen, value, false);
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		updateStat();
		byte[] keyBytes =skipListKeySerializer.serialize(key, namespace);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
		int keyLen = keyBytes.length;
		byte[] value = skipListValueSerializer.serialize(state);

		return putNode(keyByteBuffer, 0, keyLen, value, true);
	}

	@Override
	public void remove(K key, N namespace) {
		updateStat();
		byte[] keyBytes =skipListKeySerializer.serialize(key, namespace);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
		int keyLen = keyBytes.length;

		removeNode(keyByteBuffer, 0, keyLen, false);
	}

	@Override
	public S removeAndGetOld(K key, N namespace) {
		updateStat();
		byte[] keyBytes =skipListKeySerializer.serialize(key, namespace);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
		int keyLen = keyBytes.length;

		return removeNode(keyByteBuffer, 0, keyLen, true);
	}

	@Override
	public <T> void transform(
		K key, N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		updateStat();
		byte[] keyBytes =skipListKeySerializer.serialize(key, namespace);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
		int keyLen = keyBytes.length;

		long node = getNode(keyByteBuffer, 0, keyLen);
		S oldState = node == NIL_NODE ? null : helpGetNodeState(node);
		S newState = transformation.apply(oldState, value);
		byte[] stateBytes = skipListValueSerializer.serialize(newState);
		putNode(keyByteBuffer, 0, keyLen, stateBytes, false);
	}

	// Detail implementation methods ---------------------------------------------------------------

	/**
	 * Find the node containing the given key.
	 *
	 * @param keyByteBuffer byte buffer storing the key.
	 * @param keyOffset offset of the key.
	 * @param keyLen length of the key.
	 * @return id of the node. NIL_NODE will be returned if key does no exist.
	 */
	private long getNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen) {
		int deleteCount = 0;
		long prevNode = findPredecessor(keyByteBuffer, keyOffset, keyLen, 0);
		long currentNode  = helpGetNextNode(prevNode, 0);
		long nextNode;

		int c;
		while (currentNode != NIL_NODE) {
			nextNode = helpGetNextNode(currentNode, 0);

			boolean isRemoved = isNodeRemoved(currentNode);
			if (isRemoved) {
				// remove the node physically when there is no snapshot running
				if (highestRequiredSnapshotVersion == 0 && deleteCount <= numKeysToDeleteOneTime) {
					doPhysicalRemove(currentNode, prevNode, nextNode);
					logicallyRemovedNodes.remove(currentNode);
					totalSize--;
					deleteCount++;
				} else {
					prevNode = currentNode;
				}
				currentNode = nextNode;
				continue;
			}

			c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, currentNode);

			// find the key
			if (c == 0) {
				return currentNode;
			}

			// the key is less than the current node, and nodes after current
			// node can not be equal to the key.
			if (c < 0) {
				break;
			}

			prevNode = currentNode;
			currentNode = helpGetNextNode(currentNode, 0);
		}

		return NIL_NODE;
	}

	/**
	 * Put the key into the skip list. If the key does not exist before, a new node
	 * will be created. If the key exists before, the old state will be returned.
	 *
	 * @param keyByteBuffer byte buffer storing the key.
	 * @param keyOffset offset of the key.
	 * @param keyLen length of the key.
	 * @param value the value.
	 * @param returnOldState whether to return old state.
	 * @return the old state. Null will be returned if key does not exist or returnOldState is false.
	 */
	private S putNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen, byte[] value, boolean returnOldState) {
		int deleteCount = 0;
		int currentLevel = levelIndexHeader.getTopLevel();
		long prevNode = findPredecessor(keyByteBuffer, keyOffset, keyLen, 0);
		long currentNode = helpGetNextNode(prevNode, 0);
		long nextNode;

		int c;
		for ( ; ; ) {
			if (currentLevel != NIL_NODE) {
				nextNode = helpGetNextNode(currentNode, 0);

				boolean isRemoved = isNodeRemoved(currentNode);

				// note that the key may be put again before the node is physically removed,
				// so the node still need to compare with the key although they can not be
				// physically removed here
				if (isRemoved && highestRequiredSnapshotVersion == 0 && deleteCount <= numKeysToDeleteOneTime) {
					doPhysicalRemove(currentNode, prevNode, nextNode);
					logicallyRemovedNodes.remove(currentNode);
					totalSize--;
					deleteCount++;
					currentNode = nextNode;
					continue;
				}

				c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, currentNode);

				if (c > 0) {
					prevNode = currentNode;
					currentNode = nextNode;
					continue;
				}

				if (c == 0) {
					int version = helpGetNodeNewestValueVersion(currentNode);
					boolean needCopyOnWrite = version < highestRequiredSnapshotVersion;
					long oldValuePointer;

					if (needCopyOnWrite) {
						oldValuePointer = updateValueWithCopyOnWrite(currentNode, value);
					} else {
						oldValuePointer = updateValueWithReplace(currentNode, value);
					}

					byte oldStatus = helpSetNodeStatus(currentNode, SkipListUtils.NodeStatus.PUT.getValue());
					if (oldStatus == SkipListUtils.NodeStatus.REMOVE.getValue()) {
						logicallyRemovedNodes.remove(currentNode);
					}

					S oldState = null;
					if (returnOldState) {
						oldState = helpGetState(oldValuePointer);
					}

					// for the replace, old value space need to free
					if (!needCopyOnWrite) {
						spaceAllocator.free(oldValuePointer);
					}

					return oldState;
				}
			}
			break;
		}

		int level = getRandomIndexLevel();
		levelIndexHeader.updateTopLevel(level);

		int totalMetaKeyLen = SkipListUtils.getKeyMetaLen(level) + keyLen;
		long node = this.spaceAllocator.allocate(totalMetaKeyLen);

		int totalValueLen = SkipListUtils.getValueMetaLen() + value.length;
		long valuePointer = spaceAllocator.allocate(totalValueLen);

		doWriteKey(node, level, keyByteBuffer, keyOffset, keyLen, valuePointer, currentNode);
		doWriteValue(valuePointer, value, stateMapVersion, node, NIL_VALUE_POINTER);

		helpSetNextNode(prevNode, node, 0);

		if (level > 0) {
			buildLevelIndex(node, level, keyByteBuffer, keyOffset, keyLen);
		}

		totalSize++;

		return null;
	}

	/**
	 * Remove the key from the skip list. The key can be removed logically or physically.
	 * Logical remove means put a null value whose size is 0. If the key exists before,
	 * the old value state will be returned.
	 *
	 * @param keyByteBuffer byte buffer storing the key.
	 * @param keyOffset offset of the key.
	 * @param keyLen length of the key.
	 * @param returnOldState whether to return old state.
	 * @return the old state. Null will be returned if key does not exist or returnOldState is false.
	 */
	private S removeNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen, boolean returnOldState) {
		int deleteCount = 0;
		long prevNode = findPredecessor(keyByteBuffer, keyOffset, keyLen, 0);
		long currentNode = helpGetNextNode(prevNode, 0);
		long nextNode;

		int c;
		while (currentNode != NIL_NODE) {
			nextNode = helpGetNextNode(currentNode, 0);

			boolean isRemoved = isNodeRemoved(currentNode);
			if (isRemoved && highestRequiredSnapshotVersion == 0 && deleteCount <= numKeysToDeleteOneTime) {
				doPhysicalRemove(currentNode, prevNode, nextNode);
				logicallyRemovedNodes.remove(currentNode);
				currentNode = nextNode;
				totalSize--;
				deleteCount++;
				continue;
			}

			c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, currentNode);

			if (c < 0) {
				break;
			}

			if (c > 0) {
				prevNode = currentNode;
				currentNode = nextNode;
				continue;
			}

			// if the node has been logically removed, and can not be physically
			// removed here, just return null
			if (isRemoved && highestRequiredSnapshotVersion != 0) {
				return null;
			}

			long oldValuePointer;
			boolean oldValueNeedFree;

			if (highestRequiredSnapshotVersion == 0) {
				// do physically remove only when there is no snapshot running
				oldValuePointer = doPhysicalRemoveAndGetValue(currentNode, prevNode, nextNode);
				// the node has been logically removed, and remove it from the set
				if (isRemoved) {
					logicallyRemovedNodes.remove(currentNode);
				}
				oldValueNeedFree = true;
				totalSize--;
			} else {
				int version = helpGetNodeNewestValueVersion(currentNode);
				if (version < highestRequiredSnapshotVersion) {
					// the newest-version value may be used by snapshots, and update it with copy-on-write
					oldValuePointer = updateValueWithCopyOnWrite(currentNode, null);
					oldValueNeedFree = false;
				} else {
					// replace the newest-version value.
					oldValuePointer = updateValueWithReplace(currentNode, null);
					oldValueNeedFree = true;
				}

				helpSetNodeStatus(currentNode, SkipListUtils.NodeStatus.REMOVE.getValue());
				logicallyRemovedNodes.add(currentNode);
			}

			S oldState = null;
			if (returnOldState) {
				oldState = helpGetState(oldValuePointer);
			}

			if (oldValueNeedFree) {
				spaceAllocator.free(oldValuePointer);
			}

			return oldState;
		}

		return null;
	}

	/**
	 * Find the predecessor node for the given key at the next level of given level.
	 * The key is in the byte buffer positioning at the given offset.
	 *
	 * @param keyByteBuffer byte buffer which contains the key.
	 * @param keyOffset offset of the key in the byte buffer.
	 * @param keyLen length of the key.
	 * @param level the level.
	 * @return node id before the key at the given level.
	 */
	private long findPredecessor(ByteBuffer keyByteBuffer, int keyOffset, int keyLen, int level) {
		int currentLevel = levelIndexHeader.getTopLevel();
		long currentNode = HEAD_NODE;
		long nextNode = levelIndexHeader.getNextNode(currentLevel);

		for ( ; ; ) {
			if (nextNode != NIL_NODE) {
				int c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, nextNode);
				if (c > 0) {
					currentNode = nextNode;
					nextNode = helpGetNextNode(currentNode, currentLevel);
					continue;
				}
			}
			currentLevel--;
			if (currentLevel <= level) {
				return currentNode;
			}

			nextNode = helpGetNextNode(currentNode, currentLevel);
		}
	}

	/**
	 * Find the predecessor node for the given node at the next level of given level.
	 *
	 * @param node the node.
	 * @param level the level.
	 * @return node id before the key at the given level.
	 */
	private long findPredecessor(long node, int level) {
		int currentLevel = levelIndexHeader.getTopLevel();
		long currentNode = HEAD_NODE;
		long nextNode = levelIndexHeader.getNextNode(currentLevel);

		for ( ; ; ) {
			if (nextNode != NIL_NODE) {
				int c = compareNodeAndNode(node, nextNode);
				if (c > 0) {
					currentNode = nextNode;
					nextNode = helpGetNextNode(currentNode, currentLevel);
					continue;
				}
			}
			currentLevel--;
			if (currentLevel <= level) {
				return currentNode;
			}

			nextNode = helpGetNextNode(currentNode, currentLevel);
		}
	}

	/**
	 * Build the level index for the given node, exclusive level 0.
	 *
	 * @param node the node.
	 * @param level level of the node.
	 * @param keyByteBuffer byte buffer of the key in the node.
	 * @param keyOffset offset of the key in byte buffer.
	 * @param keyLen length of the key.
	 */
	private void buildLevelIndex(long node, int level, ByteBuffer keyByteBuffer, int keyOffset, int keyLen) {
		int currLevel = level;
		long prevNode = findPredecessor(keyByteBuffer, keyOffset, keyLen, currLevel);
		long currentNode = helpGetNextNode(prevNode, currLevel);

		for ( ; ; ) {
			if (currentNode != NIL_NODE) {
				int c = compareByteBufferAndNode(keyByteBuffer, keyOffset, keyLen, currentNode);
				if (c > 0) {
					prevNode = currentNode;
					currentNode = helpGetNextNode(currentNode, currLevel);
					continue;
				}
			}

			helpSetPrevAndNextNode(node, prevNode, currentNode, currLevel);
			helpSetNextNode(prevNode, node, currLevel);
			helpSetPrevNode(currentNode, node, currLevel);

			currLevel--;
			if (currLevel == 0) {
				break;
			}

			currentNode = helpGetNextNode(prevNode, currLevel);
		}
	}

	/**
	 * Compare the first skip list key in the given byte buffer with the second skip list key in the given node.
	 *
	 * @param keyByteBuffer byte buffer storing the first key.
	 * @param keyOffset offset of the first key in byte buffer.
	 * @param keyLen length of the first key.
	 * @param targetNode the node storing the second key.
	 * @return Returns a negative integer, zero, or a positive integer as the first key is less than,
	 * equal to, or greater than the second.
	 */
	private int compareByteBufferAndNode(ByteBuffer keyByteBuffer, int keyOffset, int keyLen, long targetNode) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(targetNode));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(targetNode);
		ByteBuffer targetKeyByteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(targetKeyByteBuffer, offsetInByteBuffer);
		int targetKeyLen = SkipListUtils.getKeyLen(targetKeyByteBuffer, offsetInByteBuffer);
		int targetKeyOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		return skipListKeyComparator.compareTo(keyByteBuffer, keyOffset, keyLen,
			targetKeyByteBuffer, targetKeyOffset, targetKeyLen);
	}

	/**
	 * Compare the skip list key in the left node with the skip list key in the right node.
	 *
	 * @param leftNode the left node.
	 * @param rightNode the right node.
	 * @return Returns a negative integer, zero, or a positive integer as the left key is less than,
	 * equal to, or greater than the right.
	 */
	private int compareNodeAndNode(long leftNode, long rightNode) {
		Chunk leftChunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(leftNode));
		int leftOffsetInChunk = SpaceUtils.getChunkOffsetByAddress(leftNode);
		ByteBuffer leftBb = leftChunk.getByteBuffer(leftOffsetInChunk);
		int leftOffsetInByteBuffer = leftChunk.getOffsetInByteBuffer(leftOffsetInChunk);
		int leftLevel = SkipListUtils.getLevel(leftBb, leftOffsetInByteBuffer);
		int leftKeyLen = SkipListUtils.getKeyLen(leftBb, leftOffsetInByteBuffer);
		int leftKeyOffset = leftOffsetInByteBuffer + SkipListUtils.getKeyDataOffset(leftLevel);

		Chunk rightChunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(rightNode));
		int rightOffsetInChunk = SpaceUtils.getChunkOffsetByAddress(rightNode);
		ByteBuffer rightBb = rightChunk.getByteBuffer(rightOffsetInChunk);
		int rightOffsetInByteBuffer = rightChunk.getOffsetInByteBuffer(rightOffsetInChunk);
		int rightLevel = SkipListUtils.getLevel(rightBb, rightOffsetInByteBuffer);
		int rightKeyLen = SkipListUtils.getKeyLen(rightBb, rightOffsetInByteBuffer);
		int rightKeyOffset = rightOffsetInByteBuffer + SkipListUtils.getKeyDataOffset(rightLevel);

		return skipListKeyComparator.compareTo(leftBb, leftKeyOffset, leftKeyLen, rightBb, rightKeyOffset, rightKeyLen);
	}

	/**
	 * Compare the first namespace in the given byte buffer with the second namespace in the given node.
	 *
	 * @param namespaceByteBuffer byte buffer storing the first namespace.
	 * @param namespaceOffset offset of the first namespace in byte buffer.
	 * @param namespaceLen length of the first namespace.
	 * @param targetNode the node storing the second namespace.
	 * @return Returns a negative integer, zero, or a positive integer as the first key is less than,
	 * equal to, or greater than the second.
	 */
	private int compareNamespaceAndNode(ByteBuffer namespaceByteBuffer, int namespaceOffset, int namespaceLen, long targetNode) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(targetNode));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(targetNode);
		ByteBuffer targetKeyByteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(targetKeyByteBuffer, offsetInByteBuffer);
		int targetKeyLen = SkipListUtils.getKeyLen(targetKeyByteBuffer, offsetInByteBuffer);
		int targetKeyOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		return skipListKeyComparator.compareNamespace(namespaceByteBuffer, namespaceOffset, namespaceLen,
			targetKeyByteBuffer, targetKeyOffset, targetKeyLen);
	}

	/**
	 * Update the value of the node with copy-on-write mode. The old value will
	 * be linked after the new value, and can be still accessed.
	 *
	 * @param node the node to update.
	 * @param value the value.
	 * @return the old value pointer.
	 */
	private long updateValueWithCopyOnWrite(long node, byte[] value) {
		// a null value indicates this is a removed node
		int valueSize = value == null ? 0 : value.length;
		int totalValueLen = SkipListUtils.getValueMetaLen() + valueSize;
		long valuePointer = spaceAllocator.allocate(totalValueLen);

		Chunk nodeChunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInNodeChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer nodeByteBuffer = nodeChunk.getByteBuffer(offsetInNodeChunk);
		int offsetInNodeByteBuffer = nodeChunk.getOffsetInByteBuffer(offsetInNodeChunk);
		long oldValuePointer = SkipListUtils.getValuePointer(nodeByteBuffer, offsetInNodeByteBuffer);
		SkipListUtils.putValuePointer(nodeByteBuffer, offsetInNodeByteBuffer, valuePointer);

		doWriteValue(valuePointer, value, stateMapVersion, node, oldValuePointer);

		return oldValuePointer;
	}

	/**
	 * Update the value of the node with replace mode. The old value will be unlinked and replaced
	 * by the new value, and can not be accessed later. Note that the space of the old value
	 * is not freed here, and the caller of this method should be responsible for the space management.
	 *
	 * @param node the node whose value will be replaced.
	 * @param value the value.
	 * @return the old value pointer.
	 */
	private long updateValueWithReplace(long node, byte[] value) {
		// a null value indicates this is a removed node
		int valueSize = value == null ? 0 : value.length;
		int totalValueLen = SkipListUtils.getValueMetaLen() + valueSize;
		long valuePointer = spaceAllocator.allocate(totalValueLen);

		Chunk nodeChunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInNodeChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer nodeByteBuffer = nodeChunk.getByteBuffer(offsetInNodeChunk);
		int offsetInNodeByteBuffer = nodeChunk.getOffsetInByteBuffer(offsetInNodeChunk);

		long oldValuePointer = SkipListUtils.getValuePointer(nodeByteBuffer, offsetInNodeByteBuffer);
		long nextValuePointer = helpGetNextValuePointer(oldValuePointer);

		doWriteValue(valuePointer, value, stateMapVersion, node, nextValuePointer);

		SkipListUtils.putValuePointer(nodeByteBuffer, offsetInNodeByteBuffer, valuePointer);

		return oldValuePointer;
	}

	/**
	 * Removes the node physically, and free all space used by the key and value.
	 *
	 * @param node node to remove.
	 * @param prevNode previous node at the level 0.
	 * @param nextNode next node at the level 0.
	 */
	private void doPhysicalRemove(long node, long prevNode, long nextNode) {
		// set next node of prevNode at level 0 to nextNode
		helpSetNextNode(prevNode, nextNode, 0);

		// remove the level index for the node
		removeLevelIndex(node);

		// free space used by key and value
		long valuePointer = helpGetValuePointer(node);
		this.spaceAllocator.free(node);
		removeAllValues(valuePointer);
	}

	/**
	 * Removes the node physically, and return the newest-version value pointer.
	 * Space used by key and value will be freed here, but the space of newest-version
	 * value will not be freed, and the caller should be responsible for the free
	 * of space.
	 *
	 * @param node node to remove.
	 * @param prevNode previous node at the level 0.
	 * @param nextNode next node at the level 0.
	 * @return newest-version value pointer.
	 */
	private long doPhysicalRemoveAndGetValue(long node, long prevNode, long nextNode) {
		// set next node of prevNode at level 0 to nextNode
		helpSetNextNode(prevNode, nextNode, 0);

		// remove the level index for the node
		removeLevelIndex(node);

		// free space used by key and value
		long valuePointer = helpGetValuePointer(node);
		long nextValuePointer = helpGetNextValuePointer(valuePointer);
		this.spaceAllocator.free(node);
		removeAllValues(nextValuePointer);

		return valuePointer;
	}

	/**
	 * Remove the level index for the node from the skip list.
	 */
	private void removeLevelIndex(long node) {
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(bb, offsetInByteBuffer);

		for (int i = 1; i <= level; i++) {
			long prevNode = SkipListUtils.getIndexPrevNode(bb, offsetInByteBuffer, level, i);
			long nextNode = SkipListUtils.getIndexNextNode(bb, offsetInByteBuffer, i);
			helpSetNextNode(prevNode, nextNode, i);
			helpSetPrevNode(nextNode, prevNode, i);
		}
	}

	/**
	 * Free the space of the linked values, and the head value
	 * is pointed by the given pointer.
	 */
	private void removeAllValues(long valuePointer) {
		long nextValuePointer;
		while (valuePointer != NIL_VALUE_POINTER) {
			nextValuePointer = helpGetNextValuePointer(valuePointer);
			spaceAllocator.free(valuePointer);
			valuePointer = nextValuePointer;
		}
	}

	/**
	 * Return a random level for new node.
	 */
	private int getRandomIndexLevel() {
		int x = randomSeed;
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		randomSeed = x;
		// test highest and lowest bits
		if ((x & 0x8001) != 0) {
			return 0;
		}
		int level = 1;
		int curMax = levelIndexHeader.getTopLevel();
		x >>>= 1;
		while ((x & 1) != 0) {
			++level;
			x >>>= 1;
			// the level only be increased by step
			if (level > curMax) {
				break;
			}
		}
		return level;
	}


	/**
	 * Write the meta and data for the key to the given node.
	 *
	 * @param node the node for the key to write.
	 * @param level level of this node.
	 * @param keyByteBuffer byte buffer storing the key.
	 * @param keyOffset offset of key in byte buffer.
	 * @param keyLen length of the key.
	 * @param valuePointer pointer to value.
	 * @param nextNode next node on level 0.
	 */
	private void doWriteKey(
		long node,
		int level,
		ByteBuffer keyByteBuffer,
		int keyOffset,
		int keyLen,
		long valuePointer,
		long nextNode) {
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		SkipListUtils.putLevelAndNodeStatus(bb, offsetInByteBuffer, level, SkipListUtils.NodeStatus.PUT.getValue());
		SkipListUtils.putKeyLen(bb, offsetInByteBuffer, keyLen);
		SkipListUtils.putValuePointer(bb, offsetInByteBuffer, valuePointer);
		SkipListUtils.putNextKeyPointer(bb, offsetInByteBuffer, nextNode);
		SkipListUtils.putKeyData(bb, offsetInByteBuffer, keyByteBuffer, keyOffset, keyLen, level);
	}

	/**
	 * Write the meta and data for the value to the space where the value pointer points.
	 *
	 * @param valuePointer pointer to the space where the meta and data is written.
	 * @param value data of the value.
	 * @param version version of this value.
	 * @param keyPointer pointer to the key.
	 * @param nextValuePointer pointer to the next value.
	 */
	private void doWriteValue(
		long valuePointer,
		byte[] value,
		int version,
		long keyPointer,
		long nextValuePointer) {
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		SkipListUtils.putValueVersion(bb, offsetInByteBuffer, version);
		SkipListUtils.putKeyPointer(bb, offsetInByteBuffer, keyPointer);
		SkipListUtils.putNextValuePointer(bb, offsetInByteBuffer, nextValuePointer);
		SkipListUtils.putValueLen(bb, offsetInByteBuffer, value == null ? 0 : value.length);
		if (value != null) {
			SkipListUtils.putValueData(bb, offsetInByteBuffer, value);
		}
	}

	/**
	 * Find the first node with the given namespace.
	 *
	 * @param namespaceByteBuffer byte buffer storing the namespace.
	 * @param namespaceOffset offset of the namespace.
	 * @param namespaceLen length of the namespace.
	 * @return the first node with the given namespace.
	 *  NIL_NODE will be returned if not exist.
	 */
	private long getFirstNodeWithNamespace(ByteBuffer namespaceByteBuffer, int namespaceOffset, int namespaceLen) {
		int currentLevel = levelIndexHeader.getTopLevel();
		long prevNode = HEAD_NODE;
		long currentNode = helpGetNextNode(prevNode, currentLevel);

		int c;
		// find the predecessor node at level 0.
		for ( ; ; ) {
			if (currentNode != NIL_NODE) {
				c = compareNamespaceAndNode(namespaceByteBuffer, namespaceOffset, namespaceLen, currentNode);
				if (c > 0) {
					prevNode = currentNode;
					currentNode = helpGetNextNode(prevNode, currentLevel);
					continue;
				}
			}

			currentLevel--;
			if (currentLevel < 0) {
				break;
			}
			currentNode = helpGetNextNode(prevNode, currentLevel);
		}

		// find the first node that has not been logically removed
		while (currentNode != NIL_NODE) {
			if (isNodeRemoved(currentNode)) {
				currentNode = helpGetNextNode(currentNode, 0);
				continue;
			}

			c = compareNamespaceAndNode(namespaceByteBuffer, namespaceOffset, namespaceLen, currentNode);
			if (c == 0) {
				return currentNode;
			}

			if (c < 0) {
				break;
			}
		}

		return NIL_NODE;
	}

	/**
	 * Try to delete some nodes that has been logically removed.
	 */
	private void tryToDeleteNodesPhysically() {
		if (highestRequiredSnapshotVersion != 0) {
			return;
		}

		int threshold = (int) (totalSize * LOGICAL_REMOVED_KEYS_RATIO);
		int size = logicallyRemovedNodes.size();
		if (size > threshold) {
			deleteLogicallyRemovedNodes(size - threshold);
		}
	}

	private void deleteLogicallyRemovedNodes(int maxNodes) {
		int count = 0;
		Iterator<Long> nodeIterator = logicallyRemovedNodes.iterator();
		while (count < maxNodes && nodeIterator.hasNext()) {
			deleteNode(nodeIterator.next());
			nodeIterator.remove();
			totalSize--;
			count++;
		}
	}

	private void deleteNode(long node) {
		long prevNode = findPredecessor(node, 0);
		long currentNode = helpGetNextNode(prevNode, 0);
		while (currentNode != node) {
			prevNode = currentNode;
			currentNode = helpGetNextNode(prevNode, 0);
		}

		long nextNode = helpGetNextNode(currentNode, 0);
		doPhysicalRemove(currentNode, prevNode, nextNode);
	}

	/**
	 * Release all resource used by the map.
	 */
	private void releaseAllResource() {
		long node = levelIndexHeader.getNextNode(0);
		while (node != NIL_NODE) {
			long nextNode = helpGetNextNode(node, 0);
			long valuePointer = helpGetValuePointer(node);
			spaceAllocator.free(node);
			removeAllValues(valuePointer);
			node = nextNode;
		}
	}

	/**
	 * Returns the value pointer used by the snapshot of the given version,
	 * and useless version values will be pruned.
	 *
	 * @param snapshotVersion version of snapshot. 
	 * @return the value pointer used by the given snapshot. NIL_VALUE_POINTER
	 * 	will be returned if there is no value for this snapshot.
	 */
	long getAndPruneValue(long node, int snapshotVersion) {
		// whether the node is being pruned by some snapshot
		boolean isPruning = pruningValueNodes.add(node);
		try {
			long snapshotValuePointer = NIL_VALUE_POINTER;
			long valuePointer = helpGetValuePointer(node);
			while (valuePointer != NIL_VALUE_POINTER) {
				int version = helpGetValueVersion(valuePointer);

				// the first value whose version is less than snapshotVersion
				if (version < snapshotVersion && snapshotValuePointer == NIL_VALUE_POINTER) {
					snapshotValuePointer = valuePointer;
					if (!isPruning) {
						break;
					}
				}

				// if the version of the value is no more than highestFinishedSnapshotVersion,
				// snapshots that is running and to be run will not use the values who are
				// older than this version, so these values can be safely removed.
				if (highestFinishedSnapshotVersion >= version) {
					long nextValuePointer = helpGetNextValuePointer(valuePointer);
					if (nextValuePointer != NIL_VALUE_POINTER) {
						helpSetNextValuePointer(valuePointer, NIL_VALUE_POINTER);
						removeAllValues(nextValuePointer);
					}
					break;
				}

				valuePointer = helpGetValuePointer(valuePointer);
			}

			return snapshotValuePointer;
		} finally {
			// only remove the node from the set when this snapshot has pruned values
			if (isPruning) {
				pruningValueNodes.remove(node);
			}
		}
	}

	/**
	 * Update some statistics.
	 */
	private void updateStat() {
		requestCount++;
	}

	// Help methods ---------------------------------------------------------------

	/**
	 * Whether the node has been logically removed.
	 */
	boolean isNodeRemoved(long node) {
		if (node == NIL_NODE) {
			return false;
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);
		return SkipListUtils.getNodeStatus(bb, offsetInByteBuffer) == SkipListUtils.NodeStatus.REMOVE.getValue();
	}


	/**
	 * Set the next node of the given node at the given level.
	 */
	void helpSetNextNode(long node, long nextNode, int level) {
		if (node == HEAD_NODE) {
			levelIndexHeader.updateNextNode(level, nextNode);
			return;
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		if (level == 0) {
			SkipListUtils.putNextKeyPointer(bb, offsetInByteBuffer, nextNode);
		} else {
			SkipListUtils.putIndexNextNode(bb, offsetInByteBuffer, level, nextNode);
		}
	}

	/**
	 * Return the next of the given node at the given level.
	 */
	long helpGetNextNode(long node, int level) {
		if (node == HEAD_NODE) {
			return levelIndexHeader.getNextNode(level);
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		return level == 0 ? SkipListUtils.getNextKeyPointer(bb, offsetInByteBuffer)
			: SkipListUtils.getIndexNextNode(bb, offsetInByteBuffer, level);
	}

	/**
	 * Set the previous node of the given node at the given level. The level must be positive.
	 */
	void helpSetPrevNode(long node, long prevNode, int level) {
		Preconditions.checkArgument(node != HEAD_NODE, "head node does not have previous node");
		Preconditions.checkArgument(level > 0, "only index level have previous node");

		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int topLevel = SkipListUtils.getLevel(bb, offsetInByteBuffer);

		SkipListUtils.putIndexPrevNode(bb, offsetInByteBuffer, topLevel, level, prevNode);
	}

	/**
	 * Return the previous node of the given node at the given level. The level must be positive.
	 */
	long helpGetPrevNode(long node, int totalLevel, int level) {
		Preconditions.checkArgument(node != HEAD_NODE, "head node does not have previous node");
		Preconditions.checkArgument(level > 0, "only index level have previous node");

		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		return SkipListUtils.getIndexPrevNode(bb, offsetInByteBuffer, totalLevel, level);
	}

	/**
	 * Set the previous node and the next node of the given node at the given level.
	 * The level must be positive.
	 */
	void helpSetPrevAndNextNode(long node, long prevNode, long nextNode, int level) {
		Preconditions.checkArgument(node != HEAD_NODE, "head node does not have previous node");
		Preconditions.checkArgument(level > 0, "only index level have previous node");

		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int topLevel = SkipListUtils.getLevel(bb, offsetInByteBuffer);

		SkipListUtils.putIndexNextNode(bb, offsetInByteBuffer, level, nextNode);
		SkipListUtils.putIndexPrevNode(bb, offsetInByteBuffer, topLevel, level, prevNode);
	}

	/**
	 * Set node status to the given new status, and return old status.
	 */
	byte helpSetNodeStatus(long node, byte newStatus) {
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);
		byte oldStatus = SkipListUtils.getNodeStatus(bb, offsetInByteBuffer);
		if (oldStatus != newStatus) {
			int level = SkipListUtils.getLevel(bb, offsetInByteBuffer);
			SkipListUtils.putLevelAndNodeStatus(bb, offsetInByteBuffer, level, newStatus);
		}

		return oldStatus;
	}

	/**
	 * Returns the value pointer of the node.
	 */
	long helpGetValuePointer(long node) {
		Chunk chunk = this.spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		return SkipListUtils.getValuePointer(bb, offsetInByteBuffer);
	}

	/**
	 * Return the state of the node. null will be returned if the node is removed.
	 */
	S helpGetNodeState(long node) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);
		long valuePointer = SkipListUtils.getValuePointer(byteBuffer, offsetInByteBuffer);

		return helpGetState(valuePointer);
	}

	/**
	 * Return of the newest version of value for the node.
	 */
	int helpGetNodeNewestValueVersion(long node) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);
		long valuePointer = SkipListUtils.getValuePointer(byteBuffer, offsetInByteBuffer);

		return helpGetValueVersion(valuePointer);
	}

	/**
	 * Returns the version of the value.
	 */
	int helpGetValueVersion(long valuePointer) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		return SkipListUtils.getValueVersion(byteBuffer, offsetInByteBuffer);
	}

	/**
	 * Returns the length of the value.
	 */
	int helpGetValueLen(long valuePointer) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		return SkipListUtils.getValueLen(byteBuffer, offsetInByteBuffer);
	}

	/**
	 * Returns the next value pointer of the value.
	 */
	long helpGetNextValuePointer(long valuePointer) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		return SkipListUtils.getNextValuePointer(byteBuffer, offsetInByteBuffer);
	}

	/**
	 * Sets the next value pointer of the value.
	 */
	void helpSetNextValuePointer(long valuePointer, long nextValuePointer) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		SkipListUtils.putNextValuePointer(byteBuffer, offsetInByteBuffer, nextValuePointer);
	}

	/**
	 * Returns the key of the node.
	 */
	K helpGetKey(long node) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(byteBuffer, offsetInByteBuffer);
		int keyDataLen = SkipListUtils.getKeyLen(byteBuffer, offsetInByteBuffer);
		int KeyDataOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		return skipListKeySerializer.deserializeKey(byteBuffer, KeyDataOffset, keyDataLen);
	}

	/**
	 * Return the state pointed by the given pointer. The value will be deserialized
	 * with the given serializer.
	 */
	S helpGetState(long valuePointer, SkipListValueSerializer<S> serializer) {
		if (valuePointer == NIL_VALUE_POINTER) {
			return null;
		}

		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(valuePointer));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(valuePointer);
		ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int valueLen = SkipListUtils.getValueLen(bb, offsetInByteBuffer);
		if (valueLen == 0) {
			// it is a removed key
			return null;
		}

		return serializer.deserializeState(bb,
			offsetInByteBuffer + SkipListUtils.getValueMetaLen(), valueLen);
	}

	/**
	 * Return the state pointed by the given pointer. The serializer used is the
	 * {@link #skipListValueSerializer}. Because serializer is not thread safe, so
	 * this method should only be called in the state map synchronously.
	 */
	S helpGetState(long valuePointer) {
		return helpGetState(valuePointer, skipListValueSerializer);
	}

	/**
	 * Returns the state entry of the node.
	 */
	private StateEntry<K, N, S> helpGetStateEntry(long node) {
		Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
		int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
		ByteBuffer byteBuffer = chunk.getByteBuffer(offsetInChunk);
		int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

		int level = SkipListUtils.getLevel(byteBuffer, offsetInByteBuffer);
		int keyDataLen = SkipListUtils.getKeyLen(byteBuffer, offsetInByteBuffer);
		int KeyDataOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

		K key = skipListKeySerializer.deserializeKey(byteBuffer, KeyDataOffset, keyDataLen);
		N namespace = skipListKeySerializer.deserializeNamespace(byteBuffer, KeyDataOffset, keyDataLen);
		long valuePointer = SkipListUtils.getValuePointer(byteBuffer, offsetInByteBuffer);
		S state = helpGetState(valuePointer);

		return new StateEntry.SimpleStateEntry<>(key, namespace, state);
	}

	// ----------------------------------------------------------------------------------

	@Override
	public Stream<K> getKeys(N namespace) {
		updateStat();
		byte[] namespaceBytes = skipListKeySerializer.serializeNamespace(namespace);
		ByteBuffer namespaceByteBuffer = ByteBuffer.wrap(namespaceBytes);
		Iterator<Long> nodeIter = new NamespaceNodeIterator(namespaceByteBuffer, 0, namespaceBytes.length);
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(nodeIter, 0), false)
			.map(this::helpGetKey);
	}

	@Override
	public int sizeOfNamespace(Object namespace) {
		updateStat();
		byte[] namespaceBytes = skipListKeySerializer.serializeNamespace((N) namespace);
		ByteBuffer namespaceByteBuffer = ByteBuffer.wrap(namespaceBytes);
		Iterator<Long> nodeIter = new NamespaceNodeIterator(namespaceByteBuffer, 0, namespaceBytes.length);
		int size = 0;
		while (nodeIter.hasNext()) {
			nodeIter.next();
			size++;
		}

		return size;
	}

	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		updateStat();
		final Iterator<Long> nodeIter = new NodeIterator();
		return new Iterator<StateEntry<K, N, S>>() {
			@Override
			public boolean hasNext() {
				return nodeIter.hasNext();
			}

			@Override
			public StateEntry<K, N, S> next() {
				return helpGetStateEntry(nodeIter.next());
			}
		};
	}

	@Override
	public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return new StateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
	}

	@Nonnull
	@Override
	public StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot() {
		tryToDeleteNodesPhysically();

		ResourceGuard.Lease lease;
		try {
			lease = resourceGuard.acquireResource();
		} catch (Exception e) {
			throw new RuntimeException("Acquire resource failed, and can't make snapshot of state map", e);
		}

		synchronized (snapshotVersions) {
			// increase the map version for copy-on-write and register the snapshot
			if (++stateMapVersion < 0) {
				// this is just a safety net against overflows, but should never happen in practice (i.e., only after 2^31 snapshots)
				throw new IllegalStateException("Version count overflow. Enforcing restart.");
			}

			highestRequiredSnapshotVersion = stateMapVersion;
			snapshotVersions.add(highestRequiredSnapshotVersion);
		}

		return new CopyOnWriteSkipListStateMapSnapshot<>(this, lease);
	}

	@Override
	public void releaseSnapshot(StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> snapshotToRelease) {
		CopyOnWriteSkipListStateMapSnapshot snapshot = (CopyOnWriteSkipListStateMapSnapshot) snapshotToRelease;
		int snapshotVersion = snapshot.getSnapshotVersion();

		Preconditions.checkArgument(snapshot.isOwner(this),
			"Cannot release snapshot which is owned by a different state map.");

		synchronized (snapshotVersions) {
			Preconditions.checkState(snapshotVersions.remove(snapshotVersion), "Attempt to release unknown snapshot version");
			highestRequiredSnapshotVersion = snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
			highestFinishedSnapshotVersion = snapshotVersions.isEmpty() ? stateMapVersion : snapshotVersions.first() - 1;
		}
	}

	LevelIndexHeader getLevelIndexHeader() {
		return levelIndexHeader;
	}

	int getStateMapVersion() {
		return stateMapVersion;
	}

	@Override
	public void close() {
		if (!closed.compareAndSet(false, true)) {
			LOG.warn("State map has been closed");
			return;
		}

		// wait for all running snapshots finished
		resourceGuard.close();

		releaseAllResource();
	}

	/**
	 * Iterates all nodes in the skip list.
	 */
	class NodeIterator implements Iterator<Long> {

		private long nextNode;

		NodeIterator() {
			this.nextNode = getNextNode(HEAD_NODE);
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			return n;
		}

		@Override
		public boolean hasNext() {
			return nextNode != NIL_NODE;
		}

		@Override
		public Long next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			long node = nextNode;
			nextNode = getNextNode(node);

			return node;
		}
	}

	/**
	 * Iterates nodes with the given namespace;
	 */
	class NamespaceNodeIterator implements Iterator<Long> {

		private final ByteBuffer namespaceByteBuffer;
		private final int namespaceOffset;
		private final int namespaceLen;
		private long nextNode;

		NamespaceNodeIterator(ByteBuffer namespaceByteBuffer, int namespaceOffset, int namespaceLen) {
			this.namespaceByteBuffer = namespaceByteBuffer;
			this.namespaceOffset = namespaceOffset;
			this.namespaceLen = namespaceLen;
			this.nextNode = getFirstNodeWithNamespace(namespaceByteBuffer, namespaceOffset, namespaceLen);
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			if (n != NIL_NODE &&
				compareNamespaceAndNode(namespaceByteBuffer, namespaceOffset, namespaceLen, n) == 0) {
				return n;
			}

			return NIL_NODE;
		}

		@Override
		public boolean hasNext() {
			return nextNode != NIL_NODE;
		}

		@Override
		public Long next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			long node = nextNode;
			nextNode = getNextNode(node);

			return node;
		}
	}

	class StateIncrementalVisitor implements InternalKvState.StateIncrementalVisitor<K, N ,S> {

		private final int recommendedMaxNumberOfReturnedRecords;
		private ByteBuffer nextKeyByteBuffer;
		private int nextKeyOffset;
		private int nextKeyLen;
		private final Collection<StateEntry<K, N, S>> entryToReturn = new ArrayList<>(5);

		StateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
			this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
			init();
		}

		private void init() {
			long node = getNextNode(HEAD_NODE);
			if (node != NIL_NODE) {
				setKeyByteBuffer(node);
			}
		}

		private long getNextNode(long node) {
			long n = helpGetNextNode(node, 0);
			while (n != NIL_NODE && isNodeRemoved(n)) {
				n = helpGetNextNode(n, 0);
			}

			return n;
		}

		private void updateNextKeyByteBuffer(long node) {
			if (node != NIL_NODE) {
				node = getNextNode(node);
				if (node != NIL_NODE) {
					setKeyByteBuffer(node);
					return;
				}
			}
			 nextKeyByteBuffer = null;
		}

		private void setKeyByteBuffer(long node) {
			Chunk chunk = spaceAllocator.getChunkById(SpaceUtils.getChunkIdByAddress(node));
			int offsetInChunk = SpaceUtils.getChunkOffsetByAddress(node);
			ByteBuffer bb = chunk.getByteBuffer(offsetInChunk);
			int offsetInByteBuffer = chunk.getOffsetInByteBuffer(offsetInChunk);

			int level = SkipListUtils.getLevel(bb, offsetInByteBuffer);
			int keyLen = SkipListUtils.getKeyLen(bb, offsetInByteBuffer);
			int keyDataOffset = offsetInByteBuffer + SkipListUtils.getKeyDataOffset(level);

			ByteBuffer byteBuffer = ByteBuffer.allocate(keyLen);
			ByteBufferUtils.copyFromBufferToBuffer(bb, byteBuffer, keyDataOffset, 0, keyLen);
			nextKeyByteBuffer = byteBuffer;
			nextKeyOffset = 0;
			nextKeyLen = keyLen;
		}

		@Override
		public boolean hasNext() {
			// visitor may be held by the external for a long time, and the map
			// can be closed between two nextEntries(), so check the status of map
			return !closed.get() && nextKeyByteBuffer != null &&
				getNode(nextKeyByteBuffer, nextKeyOffset, nextKeyLen) != NIL_NODE;
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			if (nextKeyByteBuffer == null) {
				return null;
			}

			long node = getNode(nextKeyByteBuffer, nextKeyOffset, nextKeyLen);
			if (node == NIL_NODE) {
				return null;
			}

			entryToReturn.add(helpGetStateEntry(node));
			int n = 1;
			while (n < recommendedMaxNumberOfReturnedRecords) {
				node = getNextNode(node);
				if (node == NIL_NODE) {
					break;
				}
				entryToReturn.add(helpGetStateEntry(node));
				n++;
			}

			updateNextKeyByteBuffer(node);

			return entryToReturn;
		}

		@Override
		public void remove(StateEntry<K, N, S> stateEntry) {
			CopyOnWriteSkipListStateMap.this.remove(stateEntry.getKey(), stateEntry.getNamespace());
		}

		@Override
		public void update(StateEntry<K, N, S> stateEntry, S newValue) {
			CopyOnWriteSkipListStateMap.this.put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
		}
	}

	/**
	 * Serializer/deserializer used for conversion between key/namespace and skip list key.
	 * It is not thread safe.
	 */
	static class SkipListKeySerializer<K, N> {

		private final TypeSerializer<K> keySerializer;
		private final TypeSerializer<N> namespaceSerializer;
		private final ByteArrayOutputStreamWithPos outputStream;
		private final DataOutputViewStreamWrapper outputView;

		SkipListKeySerializer(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
			this.outputStream = new ByteArrayOutputStreamWithPos();
			this.outputView = new DataOutputViewStreamWrapper(outputStream);
		}

		/**
		 * Serialize the key and namespace to bytes.
		 */
		byte[] serialize(K key, N namespace) {
			try {
				outputStream.reset();

				// serialize namespace
				outputStream.setPosition(Integer.BYTES);
				namespaceSerializer.serialize(namespace, outputView);

				// serialize key
				int keyStartPos = outputStream.getPosition();
				outputStream.setPosition(keyStartPos + Integer.BYTES);
				keySerializer.serialize(key, outputView);

				byte[] result = outputStream.toByteArray();

				// set length of namespace and key
				int namespaceLen = keyStartPos - Integer.BYTES;
				int keyLen = result.length - keyStartPos - Integer.BYTES;
				ByteBuffer byteBuffer = ByteBuffer.wrap(result);
				ByteBufferUtils.putInt(byteBuffer, 0, namespaceLen);
				ByteBufferUtils.putInt(byteBuffer, keyStartPos, keyLen);

				return result;
			} catch (IOException e) {
				throw new RuntimeException("serialize key and namespace failed", e);
			}
		}

		/**
		 * Serialize the namespace to bytes.
		 */
		byte[] serializeNamespace(N namespace) {
			try {
				outputStream.reset();

				// serialize namespace
				outputStream.setPosition(Integer.BYTES);
				namespaceSerializer.serialize(namespace, outputView);

				byte[] result = outputStream.toByteArray();

				// set length of namespace
				int namespaceLen = result.length - Integer.BYTES;
				ByteBuffer byteBuffer = ByteBuffer.wrap(result);
				ByteBufferUtils.putInt(byteBuffer, 0, namespaceLen);

				return result;
			} catch (IOException e) {
				throw new RuntimeException("serialize namespace failed", e);
			}
		}

		/**
		 * Deserialize the namespace from the byte buffer which stores skip list key.
		 *
		 * @param byteBuffer the byte buffer which stores the skip list key.
		 * @param offset the start position of the skip list key in the byte buffer.
		 * @param len length of the skip list key.
		 */
		N deserializeNamespace(ByteBuffer byteBuffer, int offset, int len) {
			try {
				ByteBufferInputStreamWithPos inputStream = new ByteBufferInputStreamWithPos(byteBuffer, offset, len);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

				inputStream.setPosition(Integer.BYTES);
				return namespaceSerializer.deserialize(inputView);
			} catch (IOException e) {
				throw new RuntimeException("deserialize namespace failed", e);
			}
		}

		/**
		 * Deserialize the partition key from the byte buffer which stores skip list key.
		 *
		 * @param byteBuffer the byte buffer which stores the skip list key.
		 * @param offset the start position of the skip list key in the byte buffer.
		 * @param len length of the skip list key.
		 */
		K deserializeKey(ByteBuffer byteBuffer, int offset, int len) {
			try {
				ByteBufferInputStreamWithPos inputStream = new ByteBufferInputStreamWithPos(byteBuffer, offset, len);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

				int namespaceLen = ByteBufferUtils.toInt(byteBuffer, offset);
				inputStream.setPosition(Integer.BYTES + namespaceLen + Integer.BYTES);
				return keySerializer.deserialize(inputView);
			} catch (IOException e) {
				throw new RuntimeException("deserialize key failed", e);
			}
		}
	}

	/**
	 * Serializer/deserializer used for conversion between state and skip list value.
	 * It is not thread safe.
	 */
	static class SkipListValueSerializer<S> {

		private final TypeSerializer<S> stateSerializer;
		private final ByteArrayOutputStreamWithPos outputStream;
		private final DataOutputViewStreamWrapper outputView;

		SkipListValueSerializer(TypeSerializer<S> stateSerializer) {
			this.stateSerializer = stateSerializer;
			this.outputStream = new ByteArrayOutputStreamWithPos();
			this.outputView = new DataOutputViewStreamWrapper(outputStream);
		}

		byte[] serialize(S state) {
			try {
				outputStream.reset();
				stateSerializer.serialize(state, outputView);

				return outputStream.toByteArray();
			} catch (IOException e) {
				throw new RuntimeException("serialize key and namespace failed", e);
			}
		}

		/**
		 * Deserialize the state from the byte buffer which stores skip list value.
		 *
		 * @param byteBuffer the byte buffer which stores the skip list value.
		 * @param offset the start position of the skip list value in the byte buffer.
		 * @param len length of the skip list value.
		 */
		S deserializeState(ByteBuffer byteBuffer, int offset, int len) {
			try {
				ByteBufferInputStreamWithPos inputStream = new ByteBufferInputStreamWithPos(byteBuffer, offset, len);
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

				return stateSerializer.deserialize(inputView);
			} catch (IOException e) {
				throw new RuntimeException("deserialize key failed", e);
			}
		}
	}

	/**
	 * Comparator used for  skip list key and namespace.
	 */
	static class SkipListKeyComparator {

		/**
		 * Compares for order.  Returns a negative integer,
		 * zero, or a positive integer as the first node is less than, equal
		 * to, or greater than the second.
		 *
		 * @param left        left skip list key's ByteBuffer
		 * @param leftOffset  left skip list key's ByteBuffer's offset
		 * @param leftLen     left skip list key's ByteBuffer's len
		 * @param right       right skip list key's ByteBuffer
		 * @param rightOffset right skip list key's ByteBuffer's offset
		 * @param rightLen    right skip list key's ByteBuffer's len
		 * @return An integer result of the comparison.
		 */
		int compareTo(ByteBuffer left, int leftOffset, int leftLen, ByteBuffer right, int rightOffset, int rightLen) {
			// compare namespace
			int leftNamespaceLen = ByteBufferUtils.toInt(left, leftOffset);
			int rightNamespaceLen = ByteBufferUtils.toInt(right, rightOffset);

			int c = ByteBufferUtils.compareTo(left, leftOffset + Integer.BYTES, leftNamespaceLen,
				right, rightOffset + Integer.BYTES, rightNamespaceLen);

			if (c != 0) {
				return c;
			}

			// compare key
			int leftKeyOffset = leftOffset + Integer.BYTES + leftNamespaceLen;
			int rightKeyOffset = rightOffset + Integer.BYTES + rightNamespaceLen;
			int leftKeyLen = ByteBufferUtils.toInt(left, leftKeyOffset);
			int rightKeyLen = ByteBufferUtils.toInt(right, rightKeyOffset);

			return ByteBufferUtils.compareTo(left, leftKeyOffset + Integer.BYTES, leftKeyLen,
				right, rightKeyOffset + Integer.BYTES, rightKeyLen);
		}

		/**
		 * Compares the namespace.  Returns a negative integer,
		 * zero, or a positive integer as the first node is less than, equal
		 * to, or greater than the second.
		 *
		 * @param left        left namespace's ByteBuffer
		 * @param leftOffset  left namespace's ByteBuffer's offset
		 * @param leftLen     left namespace's ByteBuffer's len
		 * @param right       right namespace's ByteBuffer
		 * @param rightOffset right namespace's ByteBuffer's offset
		 * @param rightLen    right namespace's ByteBuffer's len
		 * @return An integer result of the comparison.
		 */
		int compareNamespace(ByteBuffer left, int leftOffset, int leftLen, ByteBuffer right, int rightOffset, int rightLen) {
			int leftNamespaceLen = ByteBufferUtils.toInt(left, leftOffset);
			int rightNamespaceLen = ByteBufferUtils.toInt(right, rightOffset);

			return ByteBufferUtils.compareTo(left, leftOffset + Integer.BYTES, leftNamespaceLen,
				right, rightOffset + Integer.BYTES, rightNamespaceLen);
		}
	}
}
