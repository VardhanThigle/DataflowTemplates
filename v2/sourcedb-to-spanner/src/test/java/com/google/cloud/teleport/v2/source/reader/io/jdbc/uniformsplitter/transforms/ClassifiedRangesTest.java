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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link ClassifiedRangesTest}. */
@RunWith(MockitoJUnitRunner.class)
public class ClassifiedRangesTest {
  @Test
  public void testClassifiedRangesBasic() {
    Range range =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Range rangeToCountOne = range.toBuilder().setColName("toCountOne").build();
    Range rangeToCountTwo = range.toBuilder().setColName("toCountTwo").build();
    Range rangeToRetain = range.toBuilder().setColName("toRetain").build();
    ColumnForBoundaryQuery columnForBoundaryQuery =
        ColumnForBoundaryQuery.builder()
            .setColumnName("colForBoundary")
            .setColumnClass(Integer.class)
            .build();
    ClassifiedRanges classifiedRanges =
        ClassifiedRanges.builder()
            .appendToCount(rangeToCountOne)
            .appendToCount(rangeToCountTwo)
            .appendToRetain(rangeToRetain)
            .appendToAddColumn(columnForBoundaryQuery)
            .build();
    assertThat(classifiedRanges.toAddColumn()).isEqualTo(ImmutableList.of(columnForBoundaryQuery));
    assertThat(classifiedRanges.toCount())
        .isEqualTo(ImmutableList.of(rangeToCountOne, rangeToCountTwo));
    assertThat(classifiedRanges.toAddColumn()).isEqualTo(ImmutableList.of(columnForBoundaryQuery));
  }
}
