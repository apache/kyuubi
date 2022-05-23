/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.result;

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.kyuubi.operation.ArrayFetchIterator;
import org.apache.kyuubi.operation.FetchIterator;

/**
 * A set of one statement execution result containing result kind, columns, rows of data and change
 * flags for streaming mode.
 */
public class ResultSet {

  private final ResultKind resultKind;
  private final List<Column> columns;
  private final FetchIterator<Row> data;

  // null in batch mode
  //
  // list of boolean in streaming mode,
  // true if the corresponding row is an append row, false if its a retract row
  private final List<Boolean> changeFlags;

  private ResultSet(
      ResultKind resultKind,
      List<Column> columns,
      FetchIterator<Row> data,
      @Nullable List<Boolean> changeFlags) {
    this.resultKind = Preconditions.checkNotNull(resultKind, "resultKind must not be null");
    this.columns = Preconditions.checkNotNull(columns, "columns must not be null");
    this.data = Preconditions.checkNotNull(data, "data must not be null");
    this.changeFlags = changeFlags;
    if (changeFlags != null) {
      Preconditions.checkArgument(
          Iterators.size((Iterator<?>) data) == changeFlags.size(),
          "the size of data and the size of changeFlags should be equal");
    }
  }

  public List<Column> getColumns() {
    return columns;
  }

  public FetchIterator<Row> getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResultSet resultSet = (ResultSet) o;
    return resultKind.equals(resultSet.resultKind)
        && columns.equals(resultSet.columns)
        && data.equals(resultSet.data)
        && Objects.equals(changeFlags, resultSet.changeFlags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resultKind, columns, data, changeFlags);
  }

  @Override
  public String toString() {
    return "ResultSet{"
        + "resultKind="
        + resultKind
        + ", columns="
        + columns
        + ", data="
        + data
        + ", changeFlags="
        + changeFlags
        + '}';
  }

  public static ResultSet fromTableResult(TableResult tableResult) {
    ResolvedSchema schema = tableResult.getResolvedSchema();
    // collect all rows from table result as list
    // this is ok as TableResult contains limited rows
    List<Row> rows = new ArrayList<>();
    tableResult.collect().forEachRemaining(rows::add);
    return builder()
        .resultKind(tableResult.getResultKind())
        .columns(schema.getColumns())
        .data(rows.toArray(new Row[0]))
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ResultSet}. */
  public static class Builder {
    private ResultKind resultKind = null;
    private List<Column> columns = null;
    private FetchIterator<Row> data = null;
    private List<Boolean> changeFlags = null;

    private Builder() {}

    /** Set {@link ResultKind}. */
    public Builder resultKind(ResultKind resultKind) {
      this.resultKind = resultKind;
      return this;
    }

    /** Set columns. */
    public Builder columns(Column... columns) {
      this.columns = Arrays.asList(columns);
      return this;
    }

    /** Set columns. */
    public Builder columns(List<Column> columns) {
      this.columns = columns;
      return this;
    }

    /** Set data. */
    public Builder data(FetchIterator<Row> data) {
      this.data = data;
      return this;
    }

    /** Set data. */
    public Builder data(Row[] data) {
      this.data = new ArrayFetchIterator<>(data);
      return this;
    }

    /** Set change flags. */
    public Builder changeFlags(List<Boolean> changeFlags) {
      this.changeFlags = changeFlags;
      return this;
    }

    /** Returns a {@link ResultSet} instance. */
    public ResultSet build() {
      return new ResultSet(resultKind, columns, data, changeFlags);
    }
  }
}
