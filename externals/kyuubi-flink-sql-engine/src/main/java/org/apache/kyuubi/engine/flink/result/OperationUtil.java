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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

/** Utility class for flink operation. */
public class OperationUtil {

  public static ResultSet stringListToResultSet(List<String> strings, String columnName) {
    List<Row> data = new ArrayList<>();
    boolean isNullable = false;
    int maxLength = VarCharType.DEFAULT_LENGTH;

    for (String str : strings) {
      if (str == null) {
        isNullable = true;
      } else {
        maxLength = Math.max(str.length(), maxLength);
        data.add(Row.of(str));
      }
    }

    DataType dataType = DataTypes.VARCHAR(maxLength);
    if (!isNullable) {
      dataType.notNull();
    }

    return ResultSet.builder()
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(Column.physical(columnName, dataType))
        .data(data.toArray(new Row[0]))
        .build();
  }
}
