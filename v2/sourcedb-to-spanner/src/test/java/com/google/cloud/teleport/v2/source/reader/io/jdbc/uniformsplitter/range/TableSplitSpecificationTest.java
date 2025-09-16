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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link TableSplitSpecification}. */
@RunWith(MockitoJUnitRunner.class)
public class TableSplitSpecificationTest {

  @Test
  public void testTableSplitSpecificationCreationAndAccessors() {
    TableIdentifier tableIdentifier = TableIdentifier.builder().setTableName("my_table").build();
    PartitionColumn partitionColumn =
        PartitionColumn.builder().setColumnName("id").setColumnClass(Long.class).build();
    ImmutableList<PartitionColumn> partitionColumns = ImmutableList.of(partitionColumn);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableIdentifier)
            .setPartitionColumns(partitionColumns)
            .build();

    assertThat(spec.tableIdentifier()).isEqualTo(tableIdentifier);
    assertThat(spec.partitionColumns()).isEqualTo(partitionColumns);
  }
}
