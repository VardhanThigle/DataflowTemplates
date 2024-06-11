package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Intermediate data structure generated during PTransforms to indicate a column for which a boundary (min, max) is requested.
 */
@AutoValue
public abstract class ColumnForBoundary implements Serializable {


  /**
   * @return Name of the column for which a boundary query is needed.
   */
  abstract String columnName();

  /**
   * @return Parent Range. Null indicates first column for splitting. Defaults to Null.
   */
  @Nullable
  abstract Range<?> parentRange();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setColumnName(String value);

    public abstract Builder setParentRange(Range<?> value);

    public abstract ColumnForBoundary build();
  }
}
