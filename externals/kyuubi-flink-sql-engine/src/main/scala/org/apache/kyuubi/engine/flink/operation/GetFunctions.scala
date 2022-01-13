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

package org.apache.kyuubi.engine.flink.operation

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.{DataTypes, ResultKind, TableEnvironment}
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.types.logical.VarCharType
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetFunctions(
    session: Session,
    catalogName: String,
    schemaName: String,
    functionName: String)
  extends FlinkOperation(OperationType.GET_FUNCTIONS, session) {

  override protected def runInternal(): Unit = {
    try {
      var columns: Map[String, (Int, Boolean)] = Map(
        FUNCTION_CAT -> Tuple2(VarCharType.DEFAULT_LENGTH, false),
        FUNCTION_SCHEM -> Tuple2(VarCharType.DEFAULT_LENGTH, false),
        FUNCTION_NAME -> Tuple2(VarCharType.DEFAULT_LENGTH, false))
      var rows: List[Row] = List()
      val tableEnv: TableEnvironment = sessionContext.getExecutionContext.getTableEnvironment
      tableEnv.listFunctions().diff(tableEnv.listUserDefinedFunctions())
        .filter(f => StringUtils.isEmpty(functionName) || f == functionName)
        .foreach { f =>
          columns += (FUNCTION_NAME -> Tuple2(math.max(f.length, columns(FUNCTION_NAME)._1), true))
          rows = Row.of(null, null, f) +: rows
        }
      tableEnv.listCatalogs()
        .filter(c => StringUtils.isEmpty(catalogName) || c == catalogName)
        .foreach { c =>
          columns += (FUNCTION_CAT -> Tuple2(math.max(c.length, columns(FUNCTION_CAT)._1), true))
          val catalog = tableEnv.getCatalog(c).get()
          catalog.listDatabases().asScala
            .filter(d => StringUtils.isEmpty(schemaName) || d == schemaName)
            .foreach { d =>
              columns += (
                FUNCTION_CAT -> Tuple2(math.max(d.length, columns(FUNCTION_SCHEM)._1), true))
              catalog.listFunctions(d).asScala
                .filter(f => StringUtils.isEmpty(functionName) || f == functionName)
                .foreach { f =>
                  columns += (
                    FUNCTION_NAME -> Tuple2(math.max(f.length, columns(FUNCTION_NAME)._1), true))
                  rows = Row.of(c, d, f) +: rows
                }
            }
        }
      resultSet = ResultSet.builder.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical(
            columns.head._1,
            if (columns.head._2._2) DataTypes.VARCHAR(columns.head._2._1).notNull()
            else DataTypes.VARCHAR(columns.head._2._1)),
          Column.physical(
            columns.toList(1)._1,
            if (columns.toList(1)._2._2) DataTypes.VARCHAR(columns.toList(1)._2._1).notNull()
            else DataTypes.VARCHAR(columns.toList(1)._2._1)),
          Column.physical(
            columns.last._1,
            if (columns.last._2._2) DataTypes.VARCHAR(columns.last._2._1).notNull()
            else DataTypes.VARCHAR(columns.last._2._1)))
        .data(rows.asJava.toArray(new Array[Row](0)))
        .build
    } catch {
      onError()
    }
  }
}
