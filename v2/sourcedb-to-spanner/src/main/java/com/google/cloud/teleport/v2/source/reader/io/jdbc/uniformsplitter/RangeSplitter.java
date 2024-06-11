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

/**
 * Interface to split point for generic range to split into half.
 *
 * <p>Note:
 *
 * <p>
 *
 * <ol>
 *   <li>Implementations must not assume that that start is less than end. It depends on the
 *       specific ordering used by the database schema.
 *   <li>Implementations must be overflow safe.
 * </ol>
 */
public interface RangeSplitter<T> {
  T getSplitPoint(T start, T end);
}
