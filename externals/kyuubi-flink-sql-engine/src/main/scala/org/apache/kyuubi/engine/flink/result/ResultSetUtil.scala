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

package org.apache.kyuubi.engine.flink.result;

import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.client.gateway.Executor
import org.apache.flink.table.operations.command.{ResetOperation, SetOperation}
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

  /**
   * Runs a SetOperation with executor. Returns when SetOperation is executed successfully.
   *
   * @param setOperation Set operation.
   * @param executor A gateway for communicating with Flink and other external systems.
   * @param sessionId Id of the session.
   * @return A ResultSet of SetOperation execution.
   */
  def runSetOperation(
      setOperation: SetOperation,
      executor: Executor,
      sessionId: String): ResultSet = {
    if (setOperation.getKey.isPresent) {
      val key: String = setOperation.getKey.get.trim

      if (setOperation.getValue.isPresent) {
        val newValue: String = setOperation.getValue.get.trim
        executor.setSessionProperty(sessionId, key, newValue)
      }

      val value = executor.getSessionConfigMap(sessionId).getOrDefault(key, "")
      ResultSet.builder
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical("key", DataTypes.STRING()),
          Column.physical("value", DataTypes.STRING()))
        .data(Array(Row.of(key, value)))
        .build
    } else {
      // show all properties if set without key
      val properties: util.Map[String, String] = executor.getSessionConfigMap(sessionId)

      val entries = ArrayBuffer.empty[Row]
      properties.forEach((key, value) => entries.append(Row.of(key, value)))

      if (entries.nonEmpty) {
        val prettyEntries = entries.sortBy(_.getField(0).asInstanceOf[String])
        ResultSet.builder
          .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
          .columns(
            Column.physical("key", DataTypes.STRING()),
            Column.physical("value", DataTypes.STRING()))
          .data(prettyEntries.toArray)
          .build
      } else {
        ResultSet.builder
          .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
          .columns(
            Column.physical("key", DataTypes.STRING()),
            Column.physical("value", DataTypes.STRING()))
          .data(Array[Row]())
          .build
      }
    }
  }

  /**
   * Runs a ResetOperation with executor. Returns when ResetOperation is executed successfully.
   *
   * @param resetOperation Reset operation.
   * @param executor A gateway for communicating with Flink and other external systems.
   * @param sessionId Id of the session.
   * @return A ResultSet of ResetOperation execution.
   */
  def runResetOperation(
      resetOperation: ResetOperation,
      executor: Executor,
      sessionId: String): ResultSet = {
    if (resetOperation.getKey.isPresent) {
      // reset the given property
      executor.resetSessionProperty(sessionId, resetOperation.getKey.get())
    } else {
      // reset all properties
      executor.resetSessionProperties(sessionId)
    }
    successResultSet
  }
}
