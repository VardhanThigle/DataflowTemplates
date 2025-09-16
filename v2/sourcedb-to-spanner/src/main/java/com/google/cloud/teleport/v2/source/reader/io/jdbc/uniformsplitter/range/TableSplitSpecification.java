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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;

/**
 * Defines the specification for splitting a table, including its identifier and the columns to be
 * used for partitioning.
 */
@AutoValue
public abstract class TableSplitSpecification implements Serializable {

  /**
   * Returns the identifier for the table.
   *
   * @return the table identifier.
   */
  public abstract TableIdentifier tableIdentifier();

  /**
   * Returns the list of columns to be used for partitioning.
   *
   * @return the list of partition columns.
   */
  public abstract ImmutableList<PartitionColumn> partitionColumns();

  /**
   * Creates a builder for {@link TableSplitSpecification}.
   *
   * @return a new builder instance.
   */
  public static Builder builder() {
    return new AutoValue_TableSplitSpecification.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableIdentifier(TableIdentifier value);

    public abstract Builder setPartitionColumns(ImmutableList<PartitionColumn> value);

    public abstract TableSplitSpecification build();
  }
}
