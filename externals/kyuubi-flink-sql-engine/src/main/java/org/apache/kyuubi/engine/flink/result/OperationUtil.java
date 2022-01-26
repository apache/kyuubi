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

import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.types.Row;

/** Utility class for flink operation. */
public class OperationUtil {

  public static ResultSet stringListToResultSet(List<String> strings, String columnName) {
    Row[] rows = strings.stream().map(Row::of).toArray(Row[]::new);
    return ResultSet.builder()
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(Column.physical(columnName, DataTypes.STRING()))
        .data(rows)
        .build();
  }

  /**
   * Build a simple result with OK message. Returned when SQL commands are executed successfully.
   * Noted that a new ResultSet is returned each time, because ResultSet is stateful (with its
   * cursor).
   *
   * @return A simple result with OK message.
   */
  public static ResultSet successResultSet() {
    return ResultSet.builder()
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(Column.physical("result", DataTypes.STRING()))
        .data(new Row[] {Row.of("OK")})
        .build();
  }
}
