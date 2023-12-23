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

import scala.concurrent.duration.Duration

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.gateway.service.result.ResultFetcher
import org.apache.flink.types.Row

/** Utility object for building ResultSet. */
object ResultSetUtil {

  /**
   * Build a ResultSet with a column name and a list of String values.
   *
   * @param strings list of String values
   * @param columnName name of the result column
   * @return a ResultSet with a string column
   */
  def stringListToResultSet(strings: List[String], columnName: String): ResultSet = {
    val rows: Array[Row] = strings.map(s => Row.of(s)).toArray
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(Column.physical(columnName, DataTypes.STRING))
      .data(rows)
      .build
  }

  /**
   * Build a simple ResultSet with OK message. Returned when SQL commands are executed successfully.
   * Noted that a new ResultSet is returned each time, because ResultSet is stateful (with its
   * cursor).
   *
   * @return a simple ResultSet with OK message.
   */
  def successResultSet: ResultSet =
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(Column.physical("result", DataTypes.STRING))
      .data(Array[Row](Row.of("OK")))
      .build

  def helpMessageResultSet: ResultSet =
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(Column.physical("result", DataTypes.STRING))
      .data(Array[Row](Row.of(CommandStrings.MESSAGE_HELP.toString)))
      .build

  def fromResultFetcher(
      resultFetcher: ResultFetcher,
      maxRows: Int,
      resultFetchTimeout: Duration): ResultSet = {
    if (maxRows <= 0) {
      throw new IllegalArgumentException("maxRows should be positive")
    }
    val schema = resultFetcher.getResultSchema
    val ite = new IncrementalResultFetchIterator(resultFetcher, maxRows, resultFetchTimeout)
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(schema.getColumns)
      .data(ite)
      .build
  }
}
