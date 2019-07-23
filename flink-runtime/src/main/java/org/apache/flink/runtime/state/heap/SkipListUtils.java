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

import org.apache.flink.util.Preconditions;

import java.nio.ByteBuffer;

/**
 * Utilities for skip list.
 */
public class SkipListUtils {
	static final long NIL_NODE = -1;
	static final long HEAD_NODE = -2;
	static final long NIL_VALUE_POINTER = -1;
	static final int MAX_LEVEL = 255;
	static final int DEFAULT_LEVEL = 32;
	static final int BYTE_MASK = 0xFF;

	/**
	 * node schema:
	 * - node meta
	 * -- int: level & status
	 * --   byte0 level of node in skip list
	 * --   byte1 node status
	 * --   byte2 preserve
	 * --   byte3 preserve
	 * -- int: length of key
	 * -- long: pointer to the newest value
	 * -- long: pointer to next node
	 * -- long[]: array of pointers to next node on different levels
	 * -- long[]: array of pointers to previous node on different levels
	 * - byte[]: data of key
	 */
	static final int KEY_META_OFFSET = 0;
	static final int KEY_LEN_OFFSET = KEY_META_OFFSET + Integer.BYTES;
	static final int VALUE_POINTER_OFFSET = KEY_LEN_OFFSET + Integer.BYTES;
	static final int NEXT_KEY_POINTER_OFFSET = VALUE_POINTER_OFFSET + Long.BYTES;
	static final int LEVEL_INDEX_OFFSET = NEXT_KEY_POINTER_OFFSET + Long.BYTES;


	/**
	 * Pre-compute the offset of index for different levels to dismiss the duplicated
	 * computation at runtime.
	 */
	private static final int[] INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY = new int[MAX_LEVEL + 1];

	/**
	 * Pre-compute the length of key meta for different levels to dismiss the duplicated
	 * computation at runtime.
	 */
	private static final int[] KEY_META_LEN_BY_LEVEL_ARRAY = new int[MAX_LEVEL + 1];

	static {
		for (int i = 1; i < INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY.length; i++) {
			INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[i] = LEVEL_INDEX_OFFSET + (i - 1) * Long.BYTES;
		}

		for (int i = 1; i < KEY_META_LEN_BY_LEVEL_ARRAY.length; i++) {
			KEY_META_LEN_BY_LEVEL_ARRAY[i] = LEVEL_INDEX_OFFSET + 2 * i * Long.BYTES;
		}
	}

	public static int getLevel(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + KEY_META_OFFSET) & BYTE_MASK;
	}

	public static byte getNodeStatus(ByteBuffer byteBuffer, int offset) {
		return (byte) ((ByteBufferUtils.toInt(byteBuffer, offset + KEY_META_OFFSET) >>> 8) & BYTE_MASK);
	}

	public static void putLevelAndNodeStatus(ByteBuffer byteBuffer, int offset, int level, byte status) {
		int data = ((status & BYTE_MASK) << 8) | level;
		ByteBufferUtils.putInt(byteBuffer, offset + SkipListUtils.KEY_META_OFFSET, data);
	}

	public static int getKeyLen(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + KEY_LEN_OFFSET);
	}

	public static void putKeyLen(ByteBuffer byteBuffer, int offset, int keyLen) {
		ByteBufferUtils.putInt(byteBuffer, offset + KEY_LEN_OFFSET, keyLen);
	}

	public static long getValuePointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toLong(byteBuffer, offset + VALUE_POINTER_OFFSET);
	}

	public static void putValuePointer(ByteBuffer byteBuffer, int offset, long valuePointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + VALUE_POINTER_OFFSET, valuePointer);
	}

	public static long getNextKeyPointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toLong(byteBuffer, offset + NEXT_KEY_POINTER_OFFSET);
	}

	public static void putNextKeyPointer(ByteBuffer byteBuffer, int offset, long nextKeyPointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + NEXT_KEY_POINTER_OFFSET, nextKeyPointer);
	}

	public static long getIndexNextNode(ByteBuffer byteBuffer, int offset, int level) {
		return ByteBufferUtils.toLong(byteBuffer, offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[level]);
	}

	public static void putIndexNextNode(ByteBuffer byteBuffer, int offset, int level, long node) {
		ByteBufferUtils.putLong(byteBuffer, offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[level], node);
	}

	public static long getIndexPrevNode(ByteBuffer byteBuffer, int offset, int totalLevel, int level) {
		int of = offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[totalLevel] + level * Long.BYTES;
		return ByteBufferUtils.toLong(byteBuffer, of);
	}

	public static void putIndexPrevNode(ByteBuffer byteBuffer, int offset, int totalLevel, int level, long node) {
		int of = offset + INDEX_NEXT_OFFSET_BY_LEVEL_ARRAY[totalLevel] + level * Long.BYTES;
		ByteBufferUtils.putLong(byteBuffer, of, node);
	}

	public static int getKeyMetaLen(int level) {
		Preconditions.checkArgument(level >= 0 && level < KEY_META_LEN_BY_LEVEL_ARRAY.length,
			"level " + level + " out of range [0, " + KEY_META_LEN_BY_LEVEL_ARRAY.length + ")");
		return KEY_META_LEN_BY_LEVEL_ARRAY[level];
	}

	public static int getKeyDataOffset(int level) {
		return SkipListUtils.getKeyMetaLen(level);
	}

	public static void putKeyData(ByteBuffer byteBuffer, int offset, ByteBuffer keyByteBuffer, int keyOffset, int keyLen, int level) {
		ByteBufferUtils.copyFromBufferToBuffer(keyByteBuffer, byteBuffer, keyOffset, offset + getKeyDataOffset(level), keyLen);
	}

	/**
	 * value schema
	 * - value meta
	 * -- int: version of this value to support copy on write
	 * -- long: pointer to the node
	 * -- long: pointer to next older value
	 * -- int: length of data
	 * - byte[] data of value
	 */
	static final int VALUE_META_OFFSET = 0;
	static final int VALUE_VERSION_OFFSET = VALUE_META_OFFSET;
	static final int KEY_POINTER_OFFSET = VALUE_VERSION_OFFSET + Integer.BYTES;
	static final int NEXT_VALUE_POINTER_OFFSET = KEY_POINTER_OFFSET + Long.BYTES;
	static final int VALUE_LEN_OFFSET = NEXT_VALUE_POINTER_OFFSET + Long.BYTES;
	static final int VALUE_DATA_OFFSET = VALUE_LEN_OFFSET + Integer.BYTES;

	public static int getValueVersion(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + VALUE_VERSION_OFFSET);
	}

	public static void putValueVersion(ByteBuffer byteBuffer, int offset, int version) {
		ByteBufferUtils.putInt(byteBuffer, offset + VALUE_VERSION_OFFSET, version);
	}

	public static long getKeyPointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toLong(byteBuffer, offset + KEY_POINTER_OFFSET);
	}

	public static void putKeyPointer(ByteBuffer byteBuffer, int offset, long keyPointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + KEY_POINTER_OFFSET, keyPointer);
	}

	public static long getNextValuePointer(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + NEXT_VALUE_POINTER_OFFSET);
	}

	public static void putNextValuePointer(ByteBuffer byteBuffer, int offset, long nextValuePointer) {
		ByteBufferUtils.putLong(byteBuffer, offset + NEXT_VALUE_POINTER_OFFSET, nextValuePointer);
	}

	public static int getValueLen(ByteBuffer byteBuffer, int offset) {
		return ByteBufferUtils.toInt(byteBuffer, offset + VALUE_LEN_OFFSET);
	}

	public static void putValueLen(ByteBuffer byteBuffer, int offset, int valueLen) {
		ByteBufferUtils.putInt(byteBuffer, offset + VALUE_LEN_OFFSET, valueLen);
	}

	public static void putValueData(ByteBuffer byteBuffer, int offset, byte[] value) {
		ByteBufferUtils.copyFromArrayToBuffer(byteBuffer, offset, value, 0, value.length);
	}

	public static int getValueMetaLen() {
		return VALUE_DATA_OFFSET;
	}

	/**
	 * Status of the node.
	 */
	public enum NodeStatus {

		PUT((byte) 0), REMOVE((byte) 1);

		private byte value;

		NodeStatus(byte value) {
			this.value = value;
		}

		public byte getValue() {
			return value;
		}
	}
}
