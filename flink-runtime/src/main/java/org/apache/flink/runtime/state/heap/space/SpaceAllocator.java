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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.state.heap.space.Constants.BUCKET_SIZE;
import static org.apache.flink.runtime.state.heap.space.Constants.FOUR_BYTES_BITS;
import static org.apache.flink.runtime.state.heap.space.Constants.FOUR_BYTES_MARK;
import static org.apache.flink.runtime.state.heap.space.Constants.NO_SPACE;
import static org.apache.flink.runtime.state.heap.space.SpaceUtils.getChunkIdByAddress;
import static org.apache.flink.runtime.state.heap.space.SpaceUtils.getChunkOffsetByAddress;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * this is an entrance for user to allocate the space.
 * no thread safe
 */
public class SpaceAllocator implements Allocator {
	private static final Logger LOG = LoggerFactory.getLogger(SpaceAllocator.class);
	//currently, chunks have same size.
	private volatile Chunk[] totalSpace = new Chunk[16];
	//data less than HUGE_SIZE by default 4M
	private final List<Chunk> totalSpaceForNormal = new ArrayList<>();
	//data bigger than HUGE_SIZE
	private final List<Chunk> totalSpaceForHuge = new ArrayList<>();
	private final long chunkSize;
	private final AtomicInteger chunkIdGenerator = new AtomicInteger(0);
	private final ChunkAllocator chunkAllocator;

	public SpaceAllocator(int chunkSize, boolean preAllocate) {
		checkArgument((chunkSize & chunkSize - 1) == 0, "chunkSize should be a power of 2.");
		this.chunkSize = chunkSize;
		try {
			this.chunkAllocator = getChunkAllocator(chunkSize);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		if (preAllocate) {
			init();
		}
	}

	public void init() {
		Chunk chunk = createChunk(AllocateStrategy.SmallBucket);
		totalSpaceForNormal.add(chunk);
		safeAddtotalSpace(chunk, chunk.getChunkId());
	}

	void safeAddtotalSpace(Chunk chunk, int chunkId) {
		if (chunkId >= this.totalSpace.length) {
			Chunk[] chunkTemp = new Chunk[this.totalSpace.length * 2];
			System.arraycopy(this.totalSpace, 0, chunkTemp, 0, this.totalSpace.length);
			this.totalSpace = chunkTemp;
		}
		totalSpace[chunkId] = chunk;
	}

	private ChunkAllocator getChunkAllocator(int chunkSize) {
		return new DirectBufferChunkAllocator(chunkSize);
	}

	/**
	 * @param len allocate space size
	 * @return offset, 4 bytes is chunkId, 4 bytes is offset in the chunk.
	 */
	@Override
	public long allocate(int len) {
		if (len >= BUCKET_SIZE) {
			return doAllocate(totalSpaceForHuge, len, AllocateStrategy.HugeBucket);
		} else {
			return doAllocate(totalSpaceForNormal, len, AllocateStrategy.SmallBucket);
		}
	}

	@Override
	public void free(long offset) {
		int chunkId = getChunkIdByAddress(offset);
		int interChunkOffset = getChunkOffsetByAddress(offset);
		getChunkById(chunkId).free(interChunkOffset);
	}

	public Chunk getChunkById(int chunkId) {
		return totalSpace[chunkId];
	}

	private long doAllocate(List<Chunk> chunks, int len, AllocateStrategy allocateStrategy) {
		int offset;
		for (Chunk chunk : chunks) {
			offset = chunk.allocate(len);
			if (offset != NO_SPACE) {
				return ((chunk.getChunkId() & FOUR_BYTES_MARK) << FOUR_BYTES_BITS) | (offset & FOUR_BYTES_MARK);
			}
		}
		Chunk chunk = createChunk(allocateStrategy);
		chunks.add(chunk);
		safeAddtotalSpace(chunk, chunk.getChunkId());
		offset = chunk.allocate(len);
		if (offset != NO_SPACE) {
			return ((chunk.getChunkId() & FOUR_BYTES_MARK) << FOUR_BYTES_BITS) | (offset & FOUR_BYTES_MARK);
		}
		throw new RuntimeException("Bug, no space");
	}

	private Chunk createChunk(AllocateStrategy allocateStrategy) {
		int chunkId = chunkIdGenerator.getAndIncrement();
		return this.chunkAllocator.createChunk(chunkId, allocateStrategy);
	}

	@Override
	public void close() {
		this.chunkAllocator.close();
	}
}
