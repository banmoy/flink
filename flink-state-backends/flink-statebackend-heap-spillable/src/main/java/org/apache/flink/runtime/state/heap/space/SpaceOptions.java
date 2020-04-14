/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/**
 * Configuration options for space allocation.
 */
public class SpaceOptions {

	/** Type of space used to create chunk. */
	public static final ConfigOption<String> SPACE_TYPE = ConfigOptions
		.key("state.backend.spillable.space-type")
		.defaultValue(SpaceAllocator.SpaceType.MMAP.name())
		.withDescription(String.format("Type of space used to create chunk. Options are %s (default), %s or %s.",
			SpaceAllocator.SpaceType.MMAP.name(), SpaceAllocator.SpaceType.HEAP.name(), SpaceAllocator.SpaceType.OFFHEAP.name()));

	/** Size of chunk. */
	public static final ConfigOption<MemorySize> CHUNK_SIZE = ConfigOptions
		.key("state.backend.spillable.chunk-size")
		.memoryType()
		.defaultValue(MemorySize.ofMebiBytes(512L))
		.withDescription("Size of chunk which should be a power of two and no more than Integer#MAX_VALUE.");

	/** Maximum number of mmap files that can be used. */
	public static final ConfigOption<Integer> MAX_MMAP_FILES = ConfigOptions
		.key("state.backend.spillable.max-mmap-files")
		.intType()
		.defaultValue(Integer.MAX_VALUE)
		.withDescription("Maximum number of mmap files that can be used.");
}
