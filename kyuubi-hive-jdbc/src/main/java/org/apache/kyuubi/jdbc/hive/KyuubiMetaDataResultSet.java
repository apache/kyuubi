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

package org.apache.kyuubi.jdbc.hive;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId;

public abstract class KyuubiMetaDataResultSet<M> extends KyuubiBaseResultSet {
  protected List<M> data = Collections.emptyList();

  public KyuubiMetaDataResultSet(
      final List<String> columnNames, final List<TTypeId> columnTypes, final List<M> data)
      throws SQLException {
    if (data != null) {
      this.data = new ArrayList<>(data);
    }

    if (columnTypes != null) {
      this.columnTypes = new ArrayList<>(columnTypes);
    } else {
      this.columnTypes = Collections.emptyList();
    }

    if (columnNames != null) {
      this.columnNames = new ArrayList<>(columnNames);
      this.normalizedColumnNames = normalizeColumnNames(columnNames);
    } else {
      this.columnNames = Collections.emptyList();
      this.normalizedColumnNames = Collections.emptyList();
    }
  }

  private List<String> normalizeColumnNames(final List<String> columnNames) {
    List<String> result = new ArrayList<>(columnNames.size());
    for (String colName : columnNames) {
      result.add(colName.toLowerCase());
    }
    return result;
  }

  @Override
  public void close() throws SQLException {}
}
