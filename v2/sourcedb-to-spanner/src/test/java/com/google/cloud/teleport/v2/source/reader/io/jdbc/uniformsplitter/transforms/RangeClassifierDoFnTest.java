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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link RangeClassifierDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeClassifierDoFnTest {

  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<ClassifiedRanges> classifiedRangesCaptor;
  @Mock ProcessContext mockProcessContext;

  @Test
  public void testRangeClassifierBasic() {
    Range rangeToRetainDueToCount =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .setCount(10L)
            .build();
    Range rangeToSplit =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(1000)
            .setEnd(2000)
            .setCount(1000L)
            .build();
    Range rangeToAddColumn =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2000)
            .setEnd(2000)
            .setCount(1000L)
            .build();

    Range rangeToRetainUnSplitable =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2001)
            .setEnd(2002)
            .build()
            .withChildRange(
                Range.builder()
                    .setColName("col2")
                    .setColClass(Integer.class)
                    .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                    .setStart(1000)
                    .setEnd(1001)
                    .setCount(1000L)
                    .build(),
                mockProcessContext);
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setApproxTableCount(10L)
            .setStageIdx(1L)
            .setMaxPartitionHint(10L)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("col2")
                        .setColumnClass(Integer.class)
                        .build()))
            .build();

    Pair<Range, Range> splitRangeToCount = rangeToSplit.split(mockProcessContext);

    rangeClassifierDoFn.processElement(
        ImmutableList.of(
            rangeToRetainDueToCount, rangeToSplit, rangeToAddColumn, rangeToRetainUnSplitable),
        mockOut,
        mockProcessContext);
    verify(mockOut, times(1)).output(classifiedRangesCaptor.capture());
    ClassifiedRanges classifiedRanges = classifiedRangesCaptor.getValue();
    assertThat(classifiedRanges.toRetain())
        .isEqualTo(ImmutableList.of(rangeToRetainDueToCount, rangeToRetainUnSplitable));
    assertThat(classifiedRanges.toCount())
        .isEqualTo(ImmutableList.of(splitRangeToCount.getLeft(), splitRangeToCount.getRight()));
    assertThat(classifiedRanges.toAddColumn())
        .isEqualTo(
            ImmutableList.of(
                ColumnForBoundaryQuery.builder()
                    .setColumnName("col2")
                    .setColumnClass(Integer.class)
                    .setParentRange(rangeToAddColumn)
                    .build()));
  }
}
