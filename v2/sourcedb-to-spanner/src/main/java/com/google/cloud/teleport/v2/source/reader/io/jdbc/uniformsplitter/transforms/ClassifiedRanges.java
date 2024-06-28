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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;

@AutoValue
public abstract class ClassifiedRanges {

  public abstract ImmutableList<Range> toCount();

  public abstract ImmutableList<ColumnForBoundaryQuery> toAddColumn();

  public abstract ImmutableList<Range> toRetain();

  public static Builder builder() {
    return new AutoValue_ClassifiedRanges.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract ImmutableList.Builder<Range> toCountBuilder();

    public abstract ImmutableList.Builder<ColumnForBoundaryQuery> toAddColumnBuilder();

    public abstract ImmutableList.Builder<Range> toRetainBuilder();

    public Builder appendToCount(Range value) {
      this.toCountBuilder().add(value);
      return this;
    }

    public Builder appendToRetain(Range value) {
      this.toRetainBuilder().add(value);
      return this;
    }

    public Builder appendToAddColumn(ColumnForBoundaryQuery value) {
      this.toAddColumnBuilder().add(value);
      return this;
    }

    public abstract ClassifiedRanges build();
  }
}
