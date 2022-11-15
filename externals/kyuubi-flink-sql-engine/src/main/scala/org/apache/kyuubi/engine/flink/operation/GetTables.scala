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

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.catalog.Column
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.engine.flink.schema.SchemaHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetTables(
    session: Session,
    catalogNameOrEmpty: String,
    schemaNamePattern: String,
    tableNamePattern: String,
    tableTypes: Set[String])
  extends FlinkOperation(session) {

  override protected def runInternal(): Unit = {
    try {
      val tableEnv = sessionContext.getExecutionContext.getTableEnvironment

      val catalogName =
        if (StringUtils.isEmpty(catalogNameOrEmpty)) tableEnv.getCurrentCatalog
        else catalogNameOrEmpty

      val schemaNameRegex = toJavaRegex(schemaNamePattern)
      val tableNameRegex = toJavaRegex(tableNamePattern)

      val tables = tableEnv.getCatalog(catalogName).asScala.toArray.flatMap { flinkCatalog =>
        SchemaHelper.getSchemasWithPattern(flinkCatalog, schemaNameRegex)
          .flatMap { schemaName =>
            SchemaHelper.getFlinkTablesWithPattern(
              flinkCatalog,
              catalogName,
              schemaName,
              tableNameRegex)
              .filter {
                case (_, None) => false
                case (_, Some(flinkTable)) => tableTypes.contains(flinkTable.getTableKind.name)
              }.map { case (tableName, flinkTable) =>
                Row.of(
                  catalogName,
                  schemaName,
                  tableName,
                  flinkTable.map(_.getTableKind.name).getOrElse(""),
                  flinkTable.map(_.getComment).getOrElse(""),
                  null,
                  null,
                  null,
                  null,
                  null)
              }
          }
      }

      resultSet = ResultSet.builder.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical(TABLE_CAT, DataTypes.STRING),
          Column.physical(TABLE_SCHEM, DataTypes.STRING),
          Column.physical(TABLE_NAME, DataTypes.STRING),
          Column.physical(TABLE_TYPE, DataTypes.STRING),
          Column.physical(REMARKS, DataTypes.STRING),
          Column.physical(TYPE_CAT, DataTypes.STRING),
          Column.physical(TYPE_SCHEM, DataTypes.STRING),
          Column.physical(TYPE_NAME, DataTypes.STRING),
          Column.physical(SELF_REFERENCING_COL_NAME, DataTypes.STRING),
          Column.physical(REF_GENERATION, DataTypes.STRING))
        .data(tables)
        .build
    } catch onError()
  }
}
