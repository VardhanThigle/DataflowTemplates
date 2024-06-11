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

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.math.BigInteger;

public class RangeSplitterFactory {
  private static final ImmutableMap<Class, RangeSplitter<?>> splittermap =
      ImmutableMap.of(
          Integer.class, (RangeSplitter<Integer>) RangeSplitterFactory::splitIntegers,
          Long.class, (RangeSplitter<Long>) RangeSplitterFactory::splitLongs,
          BigInteger.class, (RangeSplitter<BigInteger>) RangeSplitterFactory::splitBigIntegers);

  /**
   * Creates {@link RangeSplitter RangeSplitter&lt;T&gt;} for pass class {@code c} such that {@code
   * T.getClass == c}.
   *
   * @param c Class for the type of {@link RangeSplitter}
   * @return {@link RangeSplitter RangeSplitter&lt;T&gt;}
   * @throws UnsupportedOperationException if RangeSplitter is not yet implemented.
   */
  public static <T extends Serializable> RangeSplitter<T> create(Class c) {

    RangeSplitter<T> splitter = (RangeSplitter<T>) splittermap.get(c);
    if (splitter == null) {
      throw new UnsupportedOperationException("Range Splitter not implemented for class " + c);
    }
    return splitter;
  }

  private RangeSplitterFactory() {}

  private static Integer splitIntegers(Integer start, Integer end) {
    Integer low;
    Integer high;
    if (start <= end) {
      low = start;
      high = end;
    } else {
      high = start;
      low = end;
    }
    return low + ((high - low) / 2);
  }

  private static BigInteger splitBigIntegers(BigInteger start, BigInteger end) {
    BigInteger low;
    BigInteger high;
    if (start.compareTo(end) <= 0) {
      low = start;
      high = end;
    } else {
      high = start;
      low = end;
    }
    return low.add((high.subtract(low)).divide(BigInteger.TWO));
  }

  private static Long splitLongs(Long start, Long end) {
    Long low;
    Long high;
    if (start <= end) {
      low = start;
      high = end;
    } else {
      high = start;
      low = end;
    }
    return low + ((high - low) / 2);
  }
}
