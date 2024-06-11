/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.math.BigInteger;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link RangeSplitterFactory}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeSplitterFactoryTest {
  @Test
  public void testLongRangeSplitter() {
    RangeSplitter<Long> splitter = RangeSplitterFactory.create(Long.class);
    assertThat(splitter.getSplitPoint(Long.MAX_VALUE - 2, Long.MAX_VALUE))
        .isEqualTo(Long.MAX_VALUE - 1);
    assertThat(splitter.getSplitPoint(Long.MIN_VALUE, Long.MIN_VALUE + 4))
        .isEqualTo(Long.MIN_VALUE + 2);
    assertThat(splitter.getSplitPoint(10L, 0L)).isEqualTo(5L);
  }

  @Test
  public void testIntegerRangeSplitter() {
    RangeSplitter<Integer> splitter = RangeSplitterFactory.create(Integer.class);
    assertThat(splitter.getSplitPoint(Integer.MAX_VALUE - 2, Integer.MAX_VALUE))
        .isEqualTo(Integer.MAX_VALUE - 1);
    assertThat(splitter.getSplitPoint(Integer.MIN_VALUE, Integer.MIN_VALUE + 4))
        .isEqualTo(Integer.MIN_VALUE + 2);
    assertThat(splitter.getSplitPoint(10, 0)).isEqualTo(5);
  }

  @Test
  public void testBigIntegerRangeSplitter() {
    RangeSplitter<BigInteger> splitter = RangeSplitterFactory.create(BigInteger.class);
    BigInteger start = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(10L));
    BigInteger startByTwo = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(5L));
    BigInteger end = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(20L));
    BigInteger mid = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(15L));
    assertThat(splitter.getSplitPoint(start, end)).isEqualTo(mid);
    assertThat(splitter.getSplitPoint(start, BigInteger.valueOf(0L))).isEqualTo(startByTwo);
  }

  @Test
  public void testRangeSplitterFactoryExceptions() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> RangeSplitterFactory.create(GenericObjectPool.class));
  }
}
