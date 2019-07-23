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

/**
 * Chunk is a contiguous byteBuffer, or logically contiguous space .
 * Bucket is logic space in the Chunk.
 * each Chunk has a BucketAllocator to avoid fragment.
 */
public interface BucketAllocator {
	/**
	 * allocate space in a Chunk, based on Bucket mechanism to void fragment.
	 *
	 * @param len how much space need to allocate
	 * @return return the address of Chunk
	 */
	int allocate(int len);

	/**
	 * free, and check if size matches Bucket's item size.
	 *
	 * @param interChunkOffset space address to free
	 * @param dataSize         to check memory size match
	 */
	void free(int interChunkOffset, int dataSize);

	/**
	 * free, and ignore if size matches Bucket's item size.
	 *
	 * @param interChunkOffset space address to free
	 */
	void free(int interChunkOffset);

	long usedSize();
}
