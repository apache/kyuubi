/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive.arrow;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class wraps multiple ColumnVectors as a row-wise table. It provides a row view of this batch
 * so that Spark can access the data row by row. Instance of it is meant to be reused during the
 * entire data loading process. A data source may extend this class with customized logic.
 */
public class ArrowColumnarBatch implements AutoCloseable {
  protected int numRows;
  protected final ArrowColumnVector[] columns;

  // Staging row returned from `getRow`.
  protected final ArrowColumnarBatchRow row;

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after calling
   * this. This must be called at the end to clean up memory allocations.
   */
  @Override
  public void close() {
    for (ArrowColumnVector c : columns) {
      c.close();
    }
  }

  /** Returns an iterator over the rows in this batch. */
  public Iterator<ArrowColumnarBatchRow> rowIterator() {
    final int maxRows = numRows;
    final ArrowColumnarBatchRow row = new ArrowColumnarBatchRow(columns);
    return new Iterator<ArrowColumnarBatchRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public ArrowColumnarBatchRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.rowId = rowId++;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** Sets the number of rows in this batch. */
  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  /** Returns the number of columns that make up this batch. */
  public int numCols() {
    return columns.length;
  }

  /** Returns the number of rows for read, including filtered rows. */
  public int numRows() {
    return numRows;
  }

  /** Returns the column at `ordinal`. */
  public ArrowColumnVector column(int ordinal) {
    return columns[ordinal];
  }

  /** Returns the row in this batch at `rowId`. Returned row is reused across calls. */
  public ArrowColumnarBatchRow getRow(int rowId) {
    assert (rowId >= 0 && rowId < numRows);
    row.rowId = rowId;
    return row;
  }

  public ArrowColumnarBatch(ArrowColumnVector[] columns) {
    this(columns, 0);
  }

  /**
   * Create a new batch from existing column vectors.
   *
   * @param columns The columns of this batch
   * @param numRows The number of rows in this batch
   */
  public ArrowColumnarBatch(ArrowColumnVector[] columns, int numRows) {
    this.columns = columns;
    this.numRows = numRows;
    this.row = new ArrowColumnarBatchRow(columns);
  }
}
