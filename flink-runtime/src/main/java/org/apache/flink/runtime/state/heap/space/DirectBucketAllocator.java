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

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.runtime.state.heap.space.Constants.NO_SPACE;

/**
 * this is for huge data which is larger than 4M. so we do a simple compaction to avoid fragment
 * this is not thread safe
 */
public class DirectBucketAllocator implements BucketAllocator {
	//only thousands of record.
	private final SortedMap<Integer, Integer> usedOffset2Length = new TreeMap<>();
	private final SortedMap<Integer, Integer> freeOffset2Length = new TreeMap<>();
	private final int capacity;
	private int offset = 0;

	public DirectBucketAllocator(int capacity) {
		this.capacity = capacity;
	}

	@Override
	public synchronized int allocate(int len) {
		//first try to find best space
		int curOffset = findBestSpace(len);
		if (curOffset != NO_SPACE) {
			return curOffset;
		}
		if (this.offset + len > capacity) {
			return findCloseFreeSpace(len);
		}
		curOffset = this.offset;
		this.offset += len;
		this.usedOffset2Length.put(curOffset, len);
		return curOffset;
	}

	@Override
	public void free(int interChunkOffset, int dataSize) {
		if (this.usedOffset2Length.get(interChunkOffset) != dataSize) {
			throw new RuntimeException("DirectBucketAllocator free size not match");
		}

		free(interChunkOffset);
	}

	@Override
	public synchronized void free(int interChunkOffset) {
		Preconditions.checkArgument(this.usedOffset2Length.containsKey(interChunkOffset));
		this.freeOffset2Length.put(interChunkOffset, this.usedOffset2Length.remove(interChunkOffset));
	}

	@Override
	public long usedSize() {
		return this.usedOffset2Length.entrySet().stream().map(Map.Entry::getValue).reduce(0, (x, y) -> x + y);
	}

	private int findBestSpace(int len) {
		Optional<Map.Entry<Integer, Integer>> result =
			this.freeOffset2Length.entrySet().stream().filter(i -> i.getValue() == len).findFirst();
		if (result.isPresent()) {
			int interChunkOffset = result.get().getKey();
			this.usedOffset2Length.put(interChunkOffset, this.freeOffset2Length.remove(interChunkOffset));
			return interChunkOffset;
		}
		return NO_SPACE;
	}

	private int findCloseFreeSpace(int len) {
		//first try to merge
		Integer lastOffset = -1;
		Integer lastLen = -1;
		Map<Integer, Integer> needMerge = new HashMap<>();
		Iterator<Map.Entry<Integer, Integer>> iterator = this.freeOffset2Length.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<Integer, Integer> freeEntry = iterator.next();

			if (lastOffset != -1 && lastOffset + lastLen == freeEntry.getKey()) {
				//find offsets can be merged
				lastLen = lastLen + freeEntry.getValue();
				needMerge.put(lastOffset, lastLen);
				iterator.remove();
				continue;
			}

			lastOffset = freeEntry.getKey();
			lastLen = freeEntry.getValue();
		}

		needMerge.entrySet().forEach(i -> this.freeOffset2Length.put(i.getKey(), i.getValue()));
		//find max space
		Optional<Map.Entry<Integer, Integer>> result =
			this.freeOffset2Length.entrySet().stream().reduce((x, y) -> x.getValue() > y.getValue() ? x : y);
		if (result.isPresent()) {
			int interChunkOffset = result.get().getKey();
			int interChunkLen = result.get().getValue();
			if (interChunkLen > len) {
				this.freeOffset2Length.remove(interChunkOffset);
				this.usedOffset2Length.put(interChunkOffset, len);
				this.freeOffset2Length.put(interChunkOffset + len, interChunkLen - len);
				return interChunkOffset;
			}
		}
		return NO_SPACE;
	}

	@Override
	public String toString() {
		Integer free = this.freeOffset2Length.entrySet().stream().map(Map.Entry::getValue).reduce(0, (x, y) -> x + y);
		Integer used = this.usedOffset2Length.entrySet().stream().map(Map.Entry::getValue).reduce(0, (x, y) -> x + y);
		return "DirectBucketAllocator used=" + used + ";free=" + free + ";left=" + (capacity - offset);
	}
}
