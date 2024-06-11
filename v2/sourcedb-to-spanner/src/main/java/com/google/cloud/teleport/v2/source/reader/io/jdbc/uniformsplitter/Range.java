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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;

/** Represents a range of rows to read from. */
@AutoValue
public abstract class Range<T extends Serializable> implements Serializable {

  /** Indicator that a range is not yet counted or that the count has timed out. */
  private static final long INDETERMINATE_COUNT = Long.MAX_VALUE;

  /**
   * @return column associated with the range.
   */
  abstract String colName();

  /**
   * @return start of the range.
   */
  abstract T start();

  /**
   * @return end of the range.
   */
  abstract T end();

  abstract RangeSplitter<T> rangeSplitter();

  /**
   * @return child range. null if there's no child range.
   */
  @Nullable
  abstract Range<?> childRange();

  /**
   * Count of a given range. Defaults to {@link Range#INDETERMINATE_COUNT}.
   *
   * @return count of rows represented by the range.
   */
  abstract long count();

  /**
   * Height for this range. The leaf child always has a height of 0. Defaults to 0.
   *
   * @return split height.
   */
  abstract long height();

  /**
   * Is this the first range of a given index. Helps to generate inclusive or exclusive bounds on
   * the range query.
   *
   * @return true if this is a first range of a given index.
   */
  abstract boolean isFirst();

  /**
   * Is this the last range of a given index. Helps to generate inclusive or exclusive bounds on the
   * range query.
   *
   * @return true if this is the last range of a given index.
   */
  abstract boolean isLast();

  /**
   * Generate a {@link Builder} from a given {@link Range}.
   *
   * @return builder.
   */
  abstract Builder<T> toBuilder();

  /**
   * @return builder for {@link Range}.
   */
  public static <T extends Serializable> Builder<T> builder() {
    return new AutoValue_Range.Builder()
        .setCount(INDETERMINATE_COUNT)
        .setHeight(0L)
        .setIsFirst(false)
        .setIsLast(false);
  }

  /**
   * @return true if the range is not yet counted, or if the count query for this range has
   *     timedout.
   */
  public boolean isUncounted() {
    return count() == INDETERMINATE_COUNT;
  }

  /**
   * Return a cloned Range with count parameter set.
   *
   * @param count count of the range
   * @return range with count.
   */
  public Range<T> withCount(long count) {
    if (hasChildRange()) {
      return withChildRange(childRange().withCount(count));
    }
    return this.toBuilder().setCount(count).build();
  }

  /**
   * Add Counts ensuring that uncounted ranges don't lead to overflow. It is assumed that the total
   * rows moved by the migration job is less than {@link Long#MAX_VALUE}
   *
   * @param left
   * @param right
   * @return
   */
  @VisibleForTesting
  protected static long addCount(long left, long right) {
    if (left == INDETERMINATE_COUNT || right == INDETERMINATE_COUNT) {
      return INDETERMINATE_COUNT;
    }
    // Unless the ranges are not counted, we will not have counts overflow as we don't support
    // tables with (LONG.MAX / 2) rows.
    return left + right;
  }

  /**
   * Returns given range with child range added to it.
   *
   * @param childRange
   * @return Range with child range appended.
   * @param <V> type of the column that's added to the splitting.
   * @throws IllegalStateException if parent is splittable. This indicates programming error and
   *     should not be seen in production.
   * @throws IllegalArgumentException if child and parent have the same column. This indicates
   *     programming error and should not be seen in production.
   */
  public <V extends Serializable> Range<T> withChildRange(Range<V> childRange) {

    Preconditions.checkState(
        !this.isBaseSplittable(),
        "Only non-splittable Ranges can have a childRange. Range: " + this);
    Preconditions.checkArgument(
        this.colName() != childRange.colName(),
        String.format(
            "Composite ranges must be on different columns, parent = %s, child = %s",
            this, childRange));
    return this.toBuilder()
        .setChildRange(childRange)
        .setCount(childRange.count())
        .setHeight(childRange.height() + 1)
        .build();
  }

  /**
   * @return true if a given range has a child range. False otherwise.
   */
  public boolean hasChildRange() {
    return (this.childRange() != null);
  }

  /**
   * Returns true if a given Range can be split. Split always happens at the deepest child range.
   *
   * @return true if a range can be split.
   */
  public boolean isSplittable() {
    return (hasChildRange() && childRange().isSplittable()) || isBaseSplittable();
  }

  /**
   * Split a given range into 2 ranges via the {@link Range#rangeSplitter()}. The caller must ensure
   * that the range is splittable. The caller should use {@link Range#isSplittable()} to check if
   * the range is splittable before calling {@link Range#split()}.
   *
   * @return a pair of split ranges.
   * @throws IllegalArgumentException if the range is not splittable. this indicates a programming
   *     error and should not be seen in production.
   */
  public Pair<Range, Range> split() {
    if (!this.isSplittable()) {
      throw new IllegalArgumentException("Trying to split non-splittable range: " + this);
    }
    if (hasChildRange()) {
      Pair<Range, Range> splitChild = childRange().split();
      return Pair.of(
          this.withChildRange(splitChild.getLeft()), this.withChildRange(splitChild.getRight()));
    }
    T splitPoint = rangeSplitter().getSplitPoint(start(), end());
    return Pair.of(
        this.toBuilder().setEnd(splitPoint).setIsFirst(isFirst()).setIsLast(false).build(),
        this.toBuilder().setStart(splitPoint).setIsFirst(false).setIsLast(isLast()).build());
  }

  /**
   * Checks if two ranges can be merged with each other.
   *
   * @param other other range
   * @return true if ranges are mergable.
   */
  public boolean isMergable(Range<?> other) {
    if (this.hasChildRange() || other.hasChildRange()) {
      if (!this.baseEqual(other)) {
        // For merging children, for parent ranges start, end, columnName and height should be
        // equal.
        return false;
      }
      return this.childRange().isMergable(other.childRange());
    } else {
      return Objects.equal(this.end(), other.start()) || Objects.equal(this.start(), other.end());
    }
  }

  /**
   * Split a given range into 2 ranges via the {@link Range#rangeSplitter()}.
   *
   * @return a pair of split ranges.
   * @throws IllegalArgumentException if the range is not splittable. This indicates a programming
   *     error and should not be seen in production.
   */
  /**
   * Merge 2 ranges. The caller must ensure that the ranges are mergable. The caller should use
   * {@link Range#isMergable(Range)} to check if the range is mergable before calling {@link
   * Range#mergeRange(Range)}.
   *
   * @param other Range to merge
   * @return merged range
   * @throws IllegalArgumentException if ranges are not mergable. This indicates a programming error
   *     and should not be seen in production.
   */
  public Range<T> mergeRange(Range<?> other) {
    if (!this.isMergable(other)) {
      throw new IllegalArgumentException(
          "Trying to merge non-mergable ranges. this: " + this + " other: " + other);
    }
    if (this.hasChildRange()) {
      Range<?> mergedChild = this.childRange().mergeRange(other.childRange());
      return this.withChildRange(mergedChild);
    } else {
      Range<T> that = (Range<T>) other;
      if (Objects.equal(this.end(), that.start())) {
        return this.toBuilder()
            .setEnd(that.end())
            .setCount(addCount(this.count(), that.count()))
            .setIsLast(that.isLast())
            .build();
      } else {
        return this.toBuilder()
            .setStart(that.start())
            .setCount(addCount(this.count(), that.count()))
            .setIsFirst(that.isFirst())
            .build();
      }
    }
  }

  /**
   * Check Equality of ranges.
   *
   * @param obj to check equality.
   * @return true if equal.
   */
  @Override
  public boolean equals(Object obj) {
    if (!this.baseEqual(obj)) {
      return false;
    }
    Range<?> that = (Range<?>) obj;
    return Objects.equal(this.childRange(), that.childRange());
  }

  private boolean isBaseSplittable() {
    T mid = rangeSplitter().getSplitPoint(start(), end());
    return !(end().equals(mid)) && !(start().equals(mid));
  }

  private boolean baseEqual(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    Range<?> that = (Range<?>) obj;
    return Objects.equal(this.start(), that.start())
        && Objects.equal(this.end(), that.end())
        && (this.height() == that.height())
        && this.colName().equals(that.colName());
  }

  @AutoValue.Builder
  public abstract static class Builder<T extends Serializable> {

    public abstract Builder<T> setColName(String value);

    public abstract Builder<T> setStart(T value);

    public abstract Builder<T> setEnd(T value);

    public abstract Builder<T> setCount(long value);

    protected abstract Builder<T> setHeight(long value);

    public abstract Builder<T> setIsFirst(boolean value);

    public abstract Builder<T> setIsLast(boolean value);

    protected abstract Builder<T> setChildRange(Range<?> value);

    public abstract Builder<T> setRangeSplitter(RangeSplitter<T> value);

    /**
     * Build {@link Range}.
     *
     * @return range.
     */
    public abstract Range<T> build();
  }
}
