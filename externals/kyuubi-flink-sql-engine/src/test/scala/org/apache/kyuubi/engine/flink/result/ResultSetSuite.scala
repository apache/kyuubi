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

package org.apache.kyuubi.engine.flink.result

import java.time.ZoneId

import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.data.StringData
import org.apache.flink.types.Row

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.flink.schema.FlinkTRowSetGenerator

class ResultSetSuite extends KyuubiFunSuite {

  test("StringData type conversion") {
    val strings = List[String]("apache", "kyuubi", null)

    val rowsOld: Array[Row] = strings.map(s => Row.of(s)).toArray
    val resultSetOld = ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(Column.physical("str1", DataTypes.STRING))
      .data(rowsOld)
      .build

    val rowsNew: Array[Row] = strings.map(s => Row.of(StringData.fromString(s))).toArray
    val resultSetNew = ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(Column.physical("str1", DataTypes.STRING))
      .data(rowsNew)
      .build

    val timeZone = ZoneId.of("America/Los_Angeles")
    assert(new FlinkTRowSetGenerator(timeZone).toRowBasedSet(rowsNew, resultSetNew)
      === new FlinkTRowSetGenerator(timeZone).toRowBasedSet(rowsOld, resultSetOld))
    assert(new FlinkTRowSetGenerator(timeZone).toColumnBasedSet(rowsNew, resultSetNew)
      === new FlinkTRowSetGenerator(timeZone).toColumnBasedSet(rowsOld, resultSetOld))
  }
}
