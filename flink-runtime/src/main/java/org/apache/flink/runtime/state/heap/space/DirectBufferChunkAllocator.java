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

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Manage chunks allocated from direct buffer.
 */
public class DirectBufferChunkAllocator implements ChunkAllocator {

	private static final Logger LOG = LoggerFactory.getLogger(DirectBufferChunkAllocator.class);

	private final long chunkSize;

	private ArrayList<ByteBuffer> directBuffers;

	private volatile boolean closed;

	public DirectBufferChunkAllocator(int chunkSize) {
		this.chunkSize = chunkSize;
		this.directBuffers = new ArrayList<>();
		this.closed = false;
		LOG.info("Chunk size for direct buffer: {}", chunkSize);
	}

	@Override
	public Chunk createChunk(int chunkId, AllocateStrategy allocateStrategy) {
		ByteBuffer buffer = ByteBuffer.allocateDirect((int) chunkSize);
		directBuffers.add(buffer);
		LOG.info("Create a direct memory chunk with id " + chunkId);
		return new FileChunk(chunkId, buffer, allocateStrategy);
	}

	@Override
	public void close() {
		if (closed) {
			LOG.warn("DirectBufferManager has been already closed.");
			return;
		}

		closed = true;

		// best effort to release direct memory
		for (ByteBuffer buffer : directBuffers) {
			if (buffer instanceof sun.nio.ch.DirectBuffer) {
				sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
				cleaner.clean();
			}
		}

		directBuffers.clear();

		LOG.info("DirectBufferManager is closed");
	}
}
