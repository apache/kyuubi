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

package org.apache.kyuubi.jdbc.hive.cli;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TColumn;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TRowSet;
import org.apache.kyuubi.shaded.thrift.TException;
import org.apache.kyuubi.shaded.thrift.protocol.TCompactProtocol;
import org.apache.kyuubi.shaded.thrift.protocol.TProtocol;
import org.apache.kyuubi.shaded.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ColumnBasedSet. */
public class ColumnBasedSet implements RowSet {
  public static final Logger LOG = LoggerFactory.getLogger(ColumnBasedSet.class);

  private long startOffset;
  private final List<ColumnBuffer> columns;

  public ColumnBasedSet(TRowSet tRowSet) throws TException {
    columns = new ArrayList<>();
    // Use TCompactProtocol to read serialized TColumns
    if (tRowSet.isSetBinaryColumns()) {
      TProtocol protocol =
          new TCompactProtocol(
              new TIOStreamTransport(new ByteArrayInputStream(tRowSet.getBinaryColumns())));
      // Read from the stream using the protocol for each column in final schema
      for (int i = 0; i < tRowSet.getColumnCount(); i++) {
        TColumn tvalue = new TColumn();
        try {
          tvalue.read(protocol);
        } catch (TException e) {
          LOG.error(e.getMessage(), e);
          throw new TException("Error reading column value from the row set blob", e);
        }
        columns.add(new ColumnBuffer(tvalue));
      }
    } else {
      if (tRowSet.getColumns() != null) {
        for (TColumn tvalue : tRowSet.getColumns()) {
          columns.add(new ColumnBuffer(tvalue));
        }
      }
    }
    startOffset = tRowSet.getStartRowOffset();
  }

  private ColumnBasedSet(List<ColumnBuffer> columns, long startOffset) {
    this.columns = columns;
    this.startOffset = startOffset;
  }

  @Override
  public ColumnBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);

    List<ColumnBuffer> subset = new ArrayList<>();
    for (ColumnBuffer column : columns) {
      subset.add(column.extractSubset(numRows));
    }
    ColumnBasedSet result = new ColumnBasedSet(subset, startOffset);
    startOffset += numRows;
    return result;
  }

  @Override
  public int numColumns() {
    return columns.size();
  }

  @Override
  public int numRows() {
    return columns.isEmpty() ? 0 : columns.get(0).size();
  }

  @Override
  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      private int index;
      private final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return index < numRows();
      }

      @Override
      public Object[] next() {
        for (int i = 0; i < columns.size(); i++) {
          convey[i] = columns.get(i).get(index);
        }
        index++;
        return convey;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }
}
