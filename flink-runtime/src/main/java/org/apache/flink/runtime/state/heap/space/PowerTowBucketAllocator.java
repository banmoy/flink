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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.map.LinkedMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.runtime.state.heap.space.Constants.NO_SPACE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Chunk is a contiguous byteBuffer, or logically contiguous space.
 * Bucket is logic space in the Chunk.
 * each Chunk has a BucketAllocator in order to avoid fragment.
 * power of 2 record size in bucket.  waste space , but can void fragment
 * for example: record in a bucket is 16, means this bucket is used for record of which size is less or equal to 16.
 * and this will automatically allocate.
 * this is not thread safe
 */
public class PowerTowBucketAllocator implements BucketAllocator {
	private static final int MIN_BUCKET_SIZE = 32;
	private static final int MIN_BUCKET_SIZE_BITS = MathUtils.log2strict(MIN_BUCKET_SIZE);
	private final int chunkSize;
	private final int bucketSize;
	private final int bucketSizeMask;
	private final int bucketSizeBits;

	private Bucket[] allBuckets;
	//BucketSizeInfo is power of 2, the min size is 32
	//so, index 0 means  32 (2 <<< 5)
	//index 1 means  64 (2 <<< 6)
	private BucketSizeInfo[] bucketSizeInfos;

	private final ConcurrentLinkedQueue<Bucket> completeFreeBucketQueue;

	public PowerTowBucketAllocator(int chunkSize, int bucketSize) {
		this(chunkSize, bucketSize, false);
	}

	public PowerTowBucketAllocator(int chunkSize, int bucketSize, boolean roundRobin) {
		checkArgument((bucketSize & bucketSize - 1) == 0, "bucketSize should be a power of 2.");
		checkArgument((MIN_BUCKET_SIZE & MIN_BUCKET_SIZE - 1) == 0, "MIN_BUCKET_SIZE should be a power of 2.");
		this.chunkSize = chunkSize;
		this.bucketSize = bucketSize;
		this.bucketSizeMask = this.bucketSize - 1;
		this.bucketSizeBits = MathUtils.log2strict(this.bucketSize);

		this.allBuckets = new Bucket[chunkSize >>> this.bucketSizeBits];

		Preconditions.checkArgument(this.allBuckets.length > this.bucketSizeBits - MIN_BUCKET_SIZE_BITS,
			"chuck size is too small");

		this.completeFreeBucketQueue = new ConcurrentLinkedQueue<>();

		bucketSizeInfos = new BucketSizeInfo[this.bucketSizeBits - MIN_BUCKET_SIZE_BITS + 1];
		for (int i = 0; i <= this.bucketSizeBits - MIN_BUCKET_SIZE_BITS; i++) {
			bucketSizeInfos[i] = new BucketSizeInfo(i, this.completeFreeBucketQueue);
		}

		for (int i = 0; i < allBuckets.length; ++i) {
			allBuckets[i] = new Bucket(i << this.bucketSizeBits, this.bucketSize);
			this.completeFreeBucketQueue.offer(allBuckets[i]);
		}
	}

	public static int getBucketSizeInfoIndex(int size) {
		Preconditions.checkArgument(size > 0, "size of 0 is undefined");
		if (size <= MIN_BUCKET_SIZE) {
			return 0;
		}
		return MathUtils.log2strict(MathUtils.roundUpToPowerOfTwo(size)) - 5;
	}

	public static int getBucketItemSizeFromIndex(int sizeIndex) {
		return 1 << (sizeIndex + MIN_BUCKET_SIZE_BITS);
	}

	@Override
	public int allocate(int len) {
		Preconditions.checkArgument(len > 0, "size must be lagger than 0");
		int sizeIndex = getBucketSizeInfoIndex(len);
		if (sizeIndex >= this.bucketSizeInfos.length) {
			throw new RuntimeException("PowerTowBucketAllocator can't allocate size larger than bucket size");
		}
		int offset = this.bucketSizeInfos[sizeIndex].allocateBlock();
		return offset;
	}

	@Override
	public void free(int interChunkOffset, int dataSize) {
		int bucketNo = safeGetBucketNo(interChunkOffset);
		Bucket targetBucket = this.allBuckets[bucketNo];
		if (MathUtils.roundUpToPowerOfTwo(dataSize) != targetBucket.getItemAllocationSize()) {
			throw new RuntimeException("PowerTwoBucketAllocator free size not match");
		}
		bucketSizeInfos[targetBucket.sizeIndex()].freeBlock(targetBucket, interChunkOffset);
	}

	@Override
	public void free(int interChunkOffset) {
		int bucketNo = safeGetBucketNo(interChunkOffset);
		Bucket targetBucket = this.allBuckets[bucketNo];
		bucketSizeInfos[targetBucket.sizeIndex()].freeBlock(targetBucket, interChunkOffset);
	}

	@Override
	public long usedSize() {
		long usedSize = 0;
		for (BucketSizeInfo info : bucketSizeInfos) {
			IndexStatistics statistics = info.statistics();
			usedSize += statistics.itemSize * statistics.usedCount;
		}
		return usedSize;
	}

	int safeGetBucketNo(int interChunkOffset) {
		int bucketNo = interChunkOffset >>> this.bucketSizeBits;
		assert bucketNo >= 0 && bucketNo < this.allBuckets.length;
		return bucketNo;
	}

	static final class Bucket {
		private final int baseOffset;
		private final int bucketCapacity;
		private int itemAllocationSize, itemAllocationBit, itemAllocationMask, sizeIndex;
		private int itemCount;
		private int[] freeList;
		private int freeCount, usedCount;

		public Bucket(int offset, int bucketCapacity) {
			this.baseOffset = offset;
			this.bucketCapacity = bucketCapacity;
			this.sizeIndex = -1;
		}

		void reconfigure(int sizeIndex) {
			this.sizeIndex = sizeIndex;
			this.itemAllocationSize = getBucketItemSizeFromIndex(sizeIndex);
			Preconditions.checkArgument(this.itemAllocationSize <= this.bucketCapacity, "error sizeIndex");
			this.itemAllocationBit = MathUtils.log2strict(this.itemAllocationSize);
			this.itemAllocationMask = this.itemAllocationSize - 1;
			this.itemCount = this.bucketCapacity >>> this.itemAllocationBit;
			this.freeCount = this.itemCount;
			this.usedCount = 0;
			this.freeList = new int[this.itemCount];
			for (int i = 0; i < this.freeCount; ++i) {
				this.freeList[i] = i;
			}
		}

		public boolean isUninstantiated() {
			return this.sizeIndex == -1;
		}

		public int sizeIndex() {
			return this.sizeIndex;
		}

		public int getItemAllocationSize() {
			return this.itemAllocationSize;
		}

		public boolean hasFreeSpace() {
			return this.freeCount > 0;
		}

		public boolean isCompletelyFree() {
			return this.usedCount == 0;
		}

		public int freeCount() {
			return this.freeCount;
		}

		public int usedCount() {
			return this.usedCount;
		}

		public int getFreeBytes() {
			return this.freeCount << this.itemAllocationBit;
		}

		public int getUsedBytes() {
			return this.usedCount << this.itemAllocationBit;
		}

		public long getBaseOffset() {
			return this.baseOffset;
		}

		public int allocate() {
			assert this.freeCount > 0; // Else should not have been called
			assert this.sizeIndex != -1;
			++this.usedCount;
			int offset = this.baseOffset + (this.freeList[--this.freeCount] << this.itemAllocationBit);
			assert offset >= 0;
			return offset;
		}

		void free(int offset) {
			offset -= this.baseOffset;
			assert offset >= 0;
			assert offset < this.bucketCapacity;
			assert (offset & this.itemAllocationMask) == 0;
			assert usedCount > 0;
			int item = offset >>> this.itemAllocationBit;
			//TODO freeListContains is slower, because item size maybe very small. we need best way to prevent
			// duplicated freeing
//			assert !freeListContains(item);
			--usedCount;
			freeList[freeCount++] = item;
		}

		private boolean freeListContains(int blockNo) {
			for (int i = 0; i < freeCount; ++i) {
				if (freeList[i] == blockNo) {
					return true;
				}
			}
			return false;
		}
	}

	static final class BucketSizeInfo {
		// Free bucket means it has space to allocate a block;
		// Completely free bucket means it has no block.
		private LinkedMap bucketList, freeBuckets;
		private final int sizeIndex;
		private final ConcurrentLinkedQueue<Bucket> completeFreeBucketQueue;

		public BucketSizeInfo(int sizeIndex, ConcurrentLinkedQueue<Bucket> completeFreeBucketQueue) {
			this.sizeIndex = sizeIndex;
			this.bucketList = new LinkedMap();
			this.freeBuckets = new LinkedMap();
			this.completeFreeBucketQueue = completeFreeBucketQueue;
		}

		private void instantiateBucket(Bucket b) {
			assert b.isUninstantiated() || b.isCompletelyFree();
			b.reconfigure(this.sizeIndex);
			this.bucketList.put(b, b);
			this.freeBuckets.put(b, b);
		}

		public int sizeIndex() {
			return this.sizeIndex;
		}

		public synchronized int allocateBlock() {
			Bucket b = null;
			if (this.freeBuckets.size() > 0) {
				// Use up an existing one first...
				b = (Bucket) this.freeBuckets.lastKey();
			}
			if (b == null) {
				b = completeFreeBucketQueue.poll();
				if (b != null) {
					instantiateBucket(b);
				}
			}
			if (b == null) {
				return NO_SPACE;
			}
			int result = b.allocate();
			if (!b.hasFreeSpace()) {
				this.freeBuckets.remove(b);
			}
			return result;
		}

		private void removeBucket(Bucket b) {
			assert b.isCompletelyFree();
			this.bucketList.remove(b);
			this.freeBuckets.remove(b);
		}

		public synchronized void freeBlock(Bucket b, int offset) {
			assert this.bucketList.containsKey(b);
			b.free(offset);

			if (b.isCompletelyFree() && bucketList.size() > 0) {
				removeBucket(b);
				this.completeFreeBucketQueue.offer(b);
			} else if (!this.freeBuckets.containsKey(b)) {
				this.freeBuckets.put(b, b);
			}
		}

		public IndexStatistics statistics() {
			long free = 0, used = 0;
			Map map;
			synchronized (this) {
				map = new HashMap(bucketList);
			}
			for (Object obj : map.keySet()) {
				Bucket b = (Bucket) obj;
				free += b.freeCount();
				used += b.usedCount();
			}
			return new IndexStatistics(free, used, getBucketItemSizeFromIndex(sizeIndex));
		}

		@VisibleForTesting
		public LinkedMap getBucketList() {
			return bucketList;
		}

		@VisibleForTesting
		public LinkedMap getFreeBuckets() {
			return freeBuckets;
		}

		@VisibleForTesting
		public ConcurrentLinkedQueue<Bucket> getCompletelyFreeBuckets() {
			return this.completeFreeBucketQueue;
		}
	}

	static class IndexStatistics {
		private long freeCount, usedCount, itemSize, totalCount;

		public long freeCount() {
			return this.freeCount;
		}

		public long usedCount() {
			return this.usedCount;
		}

		public long totalCount() {
			return this.totalCount;
		}

		public long freeBytes() {
			return this.freeCount * itemSize;
		}

		public long usedBytes() {
			return usedCount * itemSize;
		}

		public long totalBytes() {
			return totalCount * itemSize;
		}

		public long itemSize() {
			return this.itemSize;
		}

		public IndexStatistics(long free, long used, long itemSize) {
			setTo(free, used, itemSize);
		}

		public IndexStatistics() {
			setTo(-1, -1, 0);
		}

		public void setTo(long free, long used, long itemSize) {
			this.itemSize = itemSize;
			this.freeCount = free;
			this.usedCount = used;
			this.totalCount = free + used;
		}

		@Override
		public String toString() {
			return "IndexStatistics{" + "freeCount=" + freeCount + ", usedCount=" + usedCount + ", itemSize=" + itemSize
				+ ", totalCount=" + totalCount + '}';
		}
	}

	@VisibleForTesting
	public Bucket[] getAllBuckets() {
		return allBuckets;
	}

	@VisibleForTesting
	public BucketSizeInfo[] getBucketSizeInfos() {
		return bucketSizeInfos;
	}

	@VisibleForTesting
	public ConcurrentLinkedQueue<Bucket> getCompleteFreeBucketQueue() {
		return completeFreeBucketQueue;
	}
}
