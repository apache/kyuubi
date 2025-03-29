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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import com.google.common.base.Optional;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.kyuubi.jdbc.hive.KyuubiStatement;

/** Rows implementation which buffers all rows */
class BufferedRows extends Rows {
  private final List<Row> list;
  private final Iterator<Row> iterator;
  private int columnCount;
  private int maxColumnWidth;
  private boolean cancelImmediate;

  BufferedRows(BeeLine beeLine, ResultSet rs) throws SQLException {
    this(beeLine, rs, Optional.<Integer>absent());
  }

  BufferedRows(BeeLine beeLine, ResultSet rs, Optional<Integer> limit) throws SQLException {
    super(beeLine, rs);
    cancelImmediate = beeLine.getOpts().getCancelImmediate();
    list = new ArrayList<Row>();
    columnCount = rsMeta.getColumnCount();
    list.add(new Row(columnCount));

    int numRowsBuffered = 0;
    int maxRowsBuffered = limit.or(Integer.MAX_VALUE);

    while (numRowsBuffered++ < maxRowsBuffered
        && !((rs.getStatement() instanceof KyuubiStatement)
            && ((KyuubiStatement) rs.getStatement()).getIsCancelled()
            && cancelImmediate)
        && rs.next()) {
      this.list.add(new Row(columnCount, rs));
    }

    iterator = list.iterator();
    maxColumnWidth = beeLine.getOpts().getMaxColumnWidth();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Object next() {
    return iterator.next();
  }

  @Override
  public String toString() {
    return list.toString();
  }

  @Override
  void normalizeWidths() {
    if (!list.isEmpty()) {
      int[] max = new int[columnCount];
      for (Row row : list) {
        for (int j = 0; j < columnCount; j++) {
          // if the max column width is too large, reset it to max allowed Column width
          max[j] = Math.min(Math.max(max[j], row.sizes[j] + 1), maxColumnWidth);
        }
        row.sizes = max;
      }
    }
  }
}
