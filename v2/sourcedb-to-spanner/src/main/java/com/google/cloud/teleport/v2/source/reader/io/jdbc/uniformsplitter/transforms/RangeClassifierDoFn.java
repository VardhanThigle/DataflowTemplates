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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class RangeClassifierDoFn extends DoFn<ImmutableList<Range>, ClassifiedRanges>
    implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(RangeCountDoFn.class);

  abstract ImmutableList<PartitionColumn> partitionColumns();

  abstract Long approxTableCount();

  abstract Long maxPartitionHint();

  abstract Long stageIdx();

  @ProcessElement
  public void processElement(
      @Element ImmutableList<Range> input, OutputReceiver<ClassifiedRanges> out, ProcessContext c) {

    ClassifiedRanges.Builder classifiedRangesBuilder = ClassifiedRanges.builder();

    logger.debug("Classifying ranges {} for stage {}.", input, stageIdx());

    long tableCount = approxTableCount();

    long mean = 0;

    long accumulatedCount = 0;

    // Refine the Count.
    for (Range range : input) {
      accumulatedCount = range.accumulateCount(accumulatedCount);
    }
    if (accumulatedCount != Range.INDETERMINATE_COUNT) {
      tableCount = accumulatedCount;
    }

    mean = tableCount / maxPartitionHint();

    for (Range range : input) {
      if (range.isUncounted()
          || range.count()
              > ((1 + ReadWithUniformPartitions.SPLITTER_MAX_RELATIVE_DEVIATION) * mean)) {
        if (range.isSplittable(c)) {
          Pair<Range, Range> splitPair = range.split(c);
          classifiedRangesBuilder.appendToCount(splitPair.getLeft());
          classifiedRangesBuilder.appendToCount(splitPair.getRight());
        } else {
          if (range.height() + 1 < partitionColumns().size()) {
            PartitionColumn newColumn = partitionColumns().get((int) (range.height() + 1));
            ColumnForBoundaryQuery columnForBoundaryQuery =
                ColumnForBoundaryQuery.builder()
                    .setPartitionColumn(newColumn)
                    .setParentRange(range)
                    .build();
            classifiedRangesBuilder.appendToAddColumn(columnForBoundaryQuery);
          } else {
            classifiedRangesBuilder.appendToRetain(range);
          }
        }
      } else {
        classifiedRangesBuilder.appendToRetain(range);
      }
    }
    ClassifiedRanges classifiedRanges = classifiedRangesBuilder.build();
    logger.debug("Classified ranges {} for stage {}.", classifiedRanges, stageIdx());
    out.output(classifiedRanges);
  }

  public static Builder builder() {
    return new AutoValue_RangeClassifierDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setPartitionColumns(ImmutableList<PartitionColumn> value);

    public abstract Builder setApproxTableCount(Long value);

    public abstract Builder setMaxPartitionHint(Long value);

    public abstract Builder setStageIdx(Long value);

    public abstract RangeClassifierDoFn build();
  }
}
