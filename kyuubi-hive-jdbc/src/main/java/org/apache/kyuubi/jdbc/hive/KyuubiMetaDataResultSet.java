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
import java.util.List;

public abstract class KyuubiMetaDataResultSet<M> extends KyuubiBaseResultSet {
  protected final List<M> data;

  @SuppressWarnings("unchecked")
  public KyuubiMetaDataResultSet(
      final List<String> columnNames, final List<String> columnTypes, final List<M> data)
      throws SQLException {
    if (data != null) {
      this.data = new ArrayList<>(data);
    } else {
      this.data = new ArrayList<>();
    }
    if (columnNames != null) {
      this.columnNames = new ArrayList<>(columnNames);
      this.normalizedColumnNames = new ArrayList<>();
      for (String colName : columnNames) {
        this.normalizedColumnNames.add(colName.toLowerCase());
      }
    } else {
      this.columnNames = new ArrayList<>();
      this.normalizedColumnNames = new ArrayList<>();
    }
    if (columnTypes != null) {
      this.columnTypes = new ArrayList<>(columnTypes);
    } else {
      this.columnTypes = new ArrayList<>();
    }
  }

  @Override
  public void close() throws SQLException {}
}
