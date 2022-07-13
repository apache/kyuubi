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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hive.service.rpc.thrift.TColumnValue;
import org.apache.hive.service.rpc.thrift.TRow;
import org.apache.hive.service.rpc.thrift.TRowSet;

/** RowBasedSet */
public class RowBasedSet implements RowSet {

  private final long startOffset;

  private final RemovableList<TRow> rows;

  public RowBasedSet(TRowSet tRowSet) {
    rows = new RemovableList<>(tRowSet.getRows());
    startOffset = tRowSet.getStartRowOffset();
  }

  @Override
  public int numColumns() {
    return rows.isEmpty() ? 0 : rows.get(0).getColVals().size();
  }

  @Override
  public int numRows() {
    return rows.size();
  }

  @Override
  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      final Iterator<TRow> iterator = rows.iterator();
      final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Object[] next() {
        TRow row = iterator.next();
        List<TColumnValue> values = row.getColVals();
        for (int i = 0; i < values.size(); i++) {
          convey[i] = ColumnValue.toColumnValue(values.get(i));
        }
        return convey;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  private static class RemovableList<E> extends ArrayList<E> {
    public RemovableList() {
      super();
    }

    public RemovableList(List<E> rows) {
      super(rows);
    }

    @Override
    public void removeRange(int fromIndex, int toIndex) {
      super.removeRange(fromIndex, toIndex);
    }
  }
}
