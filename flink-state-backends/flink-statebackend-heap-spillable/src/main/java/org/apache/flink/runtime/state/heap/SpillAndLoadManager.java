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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Manage state spill and load.
 */
public class SpillAndLoadManager<K> {

	private static final Logger LOG = LoggerFactory.getLogger(SpillAndLoadManager.class);

	/** For spill, we prefer to spill bigger bucket with less requests first, and retained size has higher weight. */
	private static final double WEIGHT_SPILL_RETAINED_SIZE = 0.7;
	private static final double WEIGHT_SPILL_REQUEST_RATE = -0.3;
	private static final double WEIGHT_SPILL_SUM = WEIGHT_SPILL_RETAINED_SIZE + WEIGHT_SPILL_REQUEST_RATE;

	/** For load, we prefer to load smaller bucket with more requests first, and request rate has higher weight. */
	private static final double WEIGHT_LOAD_RETAINED_SIZE = -0.3;
	private static final double WEIGHT_LOAD_REQUEST_RATE = 0.7;
	private static final double WEIGHT_LOAD_SUM = WEIGHT_LOAD_RETAINED_SIZE + WEIGHT_LOAD_REQUEST_RATE;

	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
	private final HeapStatusMonitor heapStatusMonitor;

	private final long gcTimeThreshold;
	private final float highWatermarkRatio;
	private final float spillSizeRatio;
	private final float loadStartRatio;
	private final float loadEndRatio;
	private final long triggerInterval;
	private final long resourceCheckInterval;


	private final boolean isGarbageCollectionMemoryUsageSupported;
	private final long maxMemory;
	private final long highWatermarkSize;
	private final long loadStartSize;
	private final long loadEndSize;

	private long lastResourceCheckTime;
	private long lastTriggerTime;
	private HeapStatusMonitor.MonitorResult lastMonitorResult;

	public SpillAndLoadManager(
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		HeapStatusMonitor heapStatusMonitor,
		Configuration configuration) {
		this.registeredKVStates = Preconditions.checkNotNull(registeredKVStates);
		this.heapStatusMonitor = Preconditions.checkNotNull(heapStatusMonitor);
		this.gcTimeThreshold = configuration.get(SpillableOptions.GC_TIME_THRESHOLD);
		this.highWatermarkRatio = configuration.get(SpillableOptions.HIGH_WARTERMARK_RATIO);
		this.spillSizeRatio = configuration.get(SpillableOptions.SPILL_SIZE_RATIO);
		this.loadStartRatio = configuration.get(SpillableOptions.LOAD_START_RATIO);
		this.loadEndRatio = configuration.get(SpillableOptions.LOAD_END_RATIO);
		this.triggerInterval = configuration.get(SpillableOptions.TRIGGER_INTERVAL);
		this.resourceCheckInterval = configuration.get(SpillableOptions.RESOURCE_CHECK_INTERVAL);

		this.isGarbageCollectionMemoryUsageSupported = heapStatusMonitor.isGarbageCollectionMemoryUsageSupported();
		this.maxMemory = heapStatusMonitor.getMaxMemory();
		this.highWatermarkSize = (long) (maxMemory * highWatermarkRatio);
		this.loadStartSize = (long) (maxMemory * loadStartRatio);
		this.loadEndSize = (long) (maxMemory * loadEndRatio);

		this.lastResourceCheckTime = System.currentTimeMillis();
		this.lastTriggerTime = System.currentTimeMillis();
	}

	public void checkResource() {
		long currentTime = System.currentTimeMillis();

		if (currentTime - lastResourceCheckTime < resourceCheckInterval) {
			return;
		}

		lastResourceCheckTime = currentTime;
		// getMonitorResult will access a volatile variable, so this is a heavy operation
		HeapStatusMonitor.MonitorResult monitorResult = heapStatusMonitor.getMonitorResult();

		// monitor hasn't update result
		if (lastMonitorResult != null && lastMonitorResult.getId() == monitorResult.getId()) {
			return;
		}
		lastMonitorResult = monitorResult;

		ActionResult checkResult = decideAction(monitorResult);
		if (checkResult.action == Action.NONE) {
			return;
		}

		// limit the frequence of spill/load so that monitor can update memory usage after spill/load
		if (monitorResult.getTimestamp() - lastTriggerTime < triggerInterval) {
			return;
		}

		if (checkResult.action == Action.SPILL) {
			doSpill(checkResult);
		} else {
			doLoad(checkResult);
		}

		// because spill/load may cost much time, so update trigger time after the process is finished
		lastTriggerTime = System.currentTimeMillis();
	}

	public ActionResult decideAction(HeapStatusMonitor.MonitorResult monitorResult) {
		long usedMemory = isGarbageCollectionMemoryUsageSupported ?
			monitorResult.getTotalUsedMemoryAfterGc() : monitorResult.getTotalUsedMemory();

		// 1. check whether to spill
		if (monitorResult.getGarbageCollectionTime() > gcTimeThreshold ||
			(isGarbageCollectionMemoryUsageSupported && usedMemory > highWatermarkSize)) {
			float spillRatio = usedMemory < highWatermarkSize ? spillSizeRatio :
				(float) (usedMemory - highWatermarkSize) / usedMemory;
			return ActionResult.ofSpill(spillRatio);
		}

		// 2. check whether to load
		if (usedMemory < loadStartSize) {
			float loadRatio = (float) (loadEndSize - usedMemory) / usedMemory;
			return ActionResult.ofLoad(loadRatio);
		}

		return ActionResult.ofNone();
	}

	public void doSpill(ActionResult actionResult) {
		List<SpillableStateTable.StateMapMeta> onHeapStateMapMetas =
			getStateMapMetas((meta) -> meta.isOnHeap() && meta.getSize() > 0);
		if (onHeapStateMapMetas.isEmpty()) {
			LOG.debug("There is no StateMap to spill.");
			return;
		}

		sortStateMapMeta(actionResult.action, onHeapStateMapMetas);

		long totalSize = onHeapStateMapMetas.stream()
			.map(SpillableStateTable.StateMapMeta::getEstimatedMemorySize)
			.reduce(0L, (a, b) -> a + b);
		long spillSize = (long) (totalSize * actionResult.spillOrLoadRatio);

		if (spillSize == 0) {
			return;
		}

		for (SpillableStateTable.StateMapMeta meta : onHeapStateMapMetas) {
			meta.getStateTable().spillState(meta.getKeyGroupIndex());
			LOG.debug("Spill state in keygroup {} successfully", meta.getKeyGroupIndex());
			spillSize -= meta.getEstimatedMemorySize();
			if (spillSize <= 0) {
				break;
			}
		}
	}

	public void doLoad(ActionResult actionResult) {
		List<SpillableStateTable.StateMapMeta> onDiskStateMapMetas =
			getStateMapMetas((meta) -> !meta.isOnHeap() && meta.getSize() > 0);
		if (onDiskStateMapMetas.isEmpty()) {
			LOG.debug("There is no StateMap to load.");
			return;
		}

		sortStateMapMeta(actionResult.action, onDiskStateMapMetas);

		long totalSize = onDiskStateMapMetas.stream()
			.map(SpillableStateTable.StateMapMeta::getEstimatedMemorySize)
			.reduce(0L, (a, b) -> a + b);
		long loadSize = (long) (totalSize * actionResult.spillOrLoadRatio);

		if (loadSize == 0) {
			return;
		}

		for (SpillableStateTable.StateMapMeta meta : onDiskStateMapMetas) {
			loadSize -= meta.getEstimatedMemorySize();
			// if before do load so that not load more data than the expected
			if (loadSize < 0) {
				break;
			}

			meta.getStateTable().loadState(meta.getKeyGroupIndex());
			LOG.debug("Load state in keygroup {} successfully", meta.getKeyGroupIndex());
		}
	}

	private List<SpillableStateTable.StateMapMeta> getStateMapMetas(
		Function<SpillableStateTable.StateMapMeta, Boolean> stateMapFilter) {
		List<SpillableStateTable.StateMapMeta> stateMapMetas = new ArrayList<>();
		for (StateTable stateTable : registeredKVStates.values()) {
			int len = stateMapMetas.size();
			SpillableStateTable spillableStateTable = (SpillableStateTable) stateTable;
			Iterator<SpillableStateTable.StateMapMeta> iterator = spillableStateTable.stateMapIterator();
			if (iterator.hasNext()) {
				SpillableStateTable.StateMapMeta meta = iterator.next();
				if (stateMapFilter.apply(meta)) {
					stateMapMetas.add(meta);
				}
			}

			if (len < stateMapMetas.size()) {
				long estimatedSize = spillableStateTable.getStateEstimatedSize(true);
				Preconditions.checkState(estimatedSize >= 0,
					"state estimated size should be positive but is {}", estimatedSize);

				// update estimated state map memory on heap
				for (int i = len; i < stateMapMetas.size(); i++) {
					SpillableStateTable.StateMapMeta stateMapMeta = stateMapMetas.get(i);
					stateMapMeta.setEstimatedMemorySize(stateMapMeta.getSize() * estimatedSize);
				}
			}
		}

		return stateMapMetas;
	}

	public void sortStateMapMeta(Action action, List<SpillableStateTable.StateMapMeta> stateMapMetas) {
		if (stateMapMetas.isEmpty()) {
			return;
		}

		// We use formula (X - Xmin)/(Xmax - Xmin) for normalization, to make sure the normalized value range is [0,1]
		long sizeMax = 0L, sizeMin = Long.MAX_VALUE, requestMax = 0L, requestMin = Long.MAX_VALUE;
		for (SpillableStateTable.StateMapMeta meta : stateMapMetas) {
			long estimatedMemorySize = meta.getEstimatedMemorySize();
			sizeMax = Math.max(sizeMax, estimatedMemorySize);
			sizeMin = Math.min(sizeMin, estimatedMemorySize);
			long numRequests = meta.getNumRequests();
			requestMax = Math.max(requestMax, numRequests);
			requestMin = Math.min(requestMin, numRequests);
		}
		final long sizeDenominator = sizeMax - sizeMin;
		final long requestDenominator = requestMax - requestMin;
		final long sizeMinForCompare = sizeMin;
		final long requestMinForCompare = requestMin;
		final Map<SpillableStateTable.StateMapMeta, Double> computedWeights = new IdentityHashMap<>();
		Comparator<SpillableStateTable.StateMapMeta> comparator = (o1, o2) -> {
			if (o1 == o2) {
				return 0;
			}
			if (o1 == null) {
				return -1;
			}
			if (o2 == null) {
				return 1;
			}
			double weight1 = computedWeights.computeIfAbsent(o1,
				k -> computeWeight(k, action, sizeMinForCompare, requestMinForCompare, sizeDenominator,
					requestDenominator));
			double weight2 = computedWeights.computeIfAbsent(o2,
				k -> computeWeight(k, action, sizeMinForCompare, requestMinForCompare, sizeDenominator,
					requestDenominator));
			// The StateMapMeta with higher weight should be spill/load first, and we will use priority queue
			// which is a minimum heap, so we return -1 here if weight is higher
			return (weight1 > weight2) ? -1 : 1;
		};

		stateMapMetas.sort(comparator);
	}

	/**
	 * Compute the weight of the given RowMapMeta.
	 * The formula is weighted average on the normalized retained-size and request-count
	 *
	 * @param meta               the StateMapMeta to compute weight against
	 * @param action             the type of action
	 * @param sizeMin            the minimum retained-size of all RowMapMeta instances
	 * @param requestMin         the minimum request-count of all RowMapMeta instances
	 * @param sizeDenominator    the Xmax minus Xmin result of retained-size
	 * @param requestDenominator the Xmax minus Xmin result of request-count
	 * @return the computed weight
	 */
	private double computeWeight(
		SpillableStateTable.StateMapMeta meta,
		Action action,
		long sizeMin, long requestMin,
		long sizeDenominator, long requestDenominator) {
		double normalizedSize = sizeDenominator == 0L ? 0.0 : (meta.getEstimatedMemorySize() - sizeMin) / (double) sizeDenominator;
		double normalizedRequest =
			requestDenominator == 0L ? 0.0 : (meta.getNumRequests() - requestMin) / (double) requestDenominator;
		double weightRetainedSize, weightRequestRate, weightSum;
		switch (action) {
			case SPILL:
				weightRetainedSize = WEIGHT_SPILL_RETAINED_SIZE;
				weightRequestRate = WEIGHT_SPILL_REQUEST_RATE;
				weightSum = WEIGHT_SPILL_SUM;
				break;
			case LOAD:
				weightRetainedSize = WEIGHT_LOAD_RETAINED_SIZE;
				weightRequestRate = WEIGHT_LOAD_REQUEST_RATE;
				weightSum = WEIGHT_LOAD_SUM;
				break;
			default:
				throw new RuntimeException("Unsupported action: " + action);
		}
		return (weightRetainedSize * normalizedSize + weightRequestRate * normalizedRequest) / weightSum;
	}

	enum Action {
		NONE, SPILL, LOAD
	}

	static class ActionResult {
		Action action;
		float spillOrLoadRatio;

		ActionResult(Action action, float spillOrLoadRatio) {
			this.action = action;
			this.spillOrLoadRatio = spillOrLoadRatio;
		}

		static ActionResult ofNone() {
			return new ActionResult(Action.NONE, 0.0f);
		}

		static ActionResult ofSpill(float ratio) {
			return new ActionResult(Action.SPILL, ratio);
		}

		static ActionResult ofLoad(float ratio) {
			return new ActionResult(Action.LOAD, ratio);
		}
	}
}
