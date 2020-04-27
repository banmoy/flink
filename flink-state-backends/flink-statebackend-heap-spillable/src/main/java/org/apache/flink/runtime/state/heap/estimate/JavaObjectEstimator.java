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

package org.apache.flink.runtime.state.heap.estimate;

/**
 * Interface to estimate memory of java objects.
 */
public interface JavaObjectEstimator {

	/**
	 * Aligns an object size.
	 */
	long alignObjectSize(long size);

	/**
	 * Return the size of the provided {@link Long} object, returning 0 if it is
	 * cached by the JVM and its shallow size otherwise.
	 */
	long sizeOf(Long value);

	/** Returns the size in bytes of the byte[] object. */
	long sizeOf(byte[] arr);

	/** Returns the size in bytes of the boolean[] object. */
	long sizeOf(boolean[] arr);

	/** Returns the size in bytes of the char[] object. */
	long sizeOf(char[] arr);

	/** Returns the size in bytes of the short[] object. */
	long sizeOf(short[] arr);

	/** Returns the size in bytes of the int[] object. */
	long sizeOf(int[] arr);

	/** Returns the size in bytes of the float[] object. */
	long sizeOf(float[] arr);

	/** Returns the size in bytes of the long[] object. */
	long sizeOf(long[] arr);

	/** Returns the size in bytes of the double[] object. */
	long sizeOf(double[] arr);

	/** Returns the size in bytes of the String[] object. */
	long sizeOf(String[] arr);

	/** Returns the size in bytes of the String object. */
	long sizeOf(String s);

	/** Same as calling <code>sizeOf(obj, DEFAULT_FILTER)</code>. */
	long sizeOf(Object obj);

	/** Returns the shallow size in bytes of the Object[] object. */
	long shallowSizeOf(Object[] arr);

	/**
	 * Estimates a "shallow" memory usage of the given object. For arrays, this will be the
	 * memory taken by array storage (no subreferences will be followed). For objects, this
	 * will be the memory taken by the fields.
	 * JVM object alignments are also applied.
	 */
	long shallowSizeOf(Object obj);
}
