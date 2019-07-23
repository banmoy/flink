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

import java.nio.ByteBuffer;

import static org.apache.flink.runtime.state.heap.space.Constants.BUCKET_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Chunk is based on a File.
 */
public class FileChunk extends AbstractChunk {
	private final BucketAllocator bucketAllocator;
	private final ByteBuffer byteBuffer;

	public FileChunk(int chunkId, ByteBuffer byteBuffer, AllocateStrategy allocateStrategy) {
		super(chunkId, byteBuffer.capacity());
		checkArgument((byteBuffer.capacity() & byteBuffer.capacity() - 1) == 0,
			"FileChunk size should be a power of 2.");
		this.byteBuffer = byteBuffer;
		switch (allocateStrategy) {
			case SmallBucket:
				this.bucketAllocator = new PowerTowBucketAllocator(byteBuffer.capacity(), BUCKET_SIZE, true);
				break;
			default:
				this.bucketAllocator = new DirectBucketAllocator(byteBuffer.capacity());
		}
	}

	@Override
	public int allocate(int len) {
		return this.bucketAllocator.allocate(len);
	}

	@Override
	public void free(int interChunkOffset) {
		this.bucketAllocator.free(interChunkOffset);
	}

	@Override
	public ByteBuffer getByteBuffer(int chunkOffset) {
		return this.byteBuffer;
	}

	@Override
	public int getOffsetInByteBuffer(int offsetInChunk) {
		return offsetInChunk;
	}

	@Override
	public long usedSize() {
		return this.bucketAllocator.usedSize();
	}
}
