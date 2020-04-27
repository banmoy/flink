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
import org.apache.flink.runtime.state.heap.estimate.AbstractStateMemoryEstimator;
import org.apache.flink.runtime.state.heap.estimate.RamUsageEstimator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for state memory estimation.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RamUsageEstimator.class)
public class StateMemoryEstimatorTest {

	@Test
	public void testKeyAndNamespaceEstimation() {

	}

	@Test
	public void testValueStateEstimation() {
	}

	@Test
	public void testMapStateEstimation() {
	}

	@Test
	public void testListStateEstimation() {
	}

	/**
	 * State memory estimator for test, and has a fixed-size state.
	 */
	static class TestStateMemoryEstimator<K, N, S> extends AbstractStateMemoryEstimator<K, N, S> {

		private final long stateSize;

		public TestStateMemoryEstimator(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer,
			long stateSize) {
			super(keySerializer, namespaceSerializer);
			this.stateSize = stateSize;
		}

		@Override
		protected long getStateSize(S state) {
			return stateSize;
		}
	}
}
