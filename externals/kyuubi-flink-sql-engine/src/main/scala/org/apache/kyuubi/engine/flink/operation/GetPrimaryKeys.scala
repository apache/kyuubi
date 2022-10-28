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

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.catalog.{Column, UniqueConstraint}
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.engine.flink.schema.SchemaHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetPrimaryKeys(
    session: Session,
    catalogNameOrEmpty: String,
    schemaNamePattern: String,
    tableNamePattern: String)
  extends FlinkOperation(session) {

  override protected def runInternal(): Unit = {
    try {
      val tableEnv = sessionContext.getExecutionContext.getTableEnvironment
      val resolver = tableEnv match {
        case impl: StreamTableEnvironmentImpl =>
          impl.getCatalogManager.getSchemaResolver
        case _ =>
          throw new UnsupportedOperationException(
            "Unsupported Operation type GetPrimaryKeys. You can execute " +
              "DESCRIBE statement instead to get primary keys column infos.")
      }

      val catalogName =
        if (StringUtils.isEmpty(catalogNameOrEmpty)) tableEnv.getCurrentCatalog
        else catalogNameOrEmpty

      val schemaNameRegex = toJavaRegex(schemaNamePattern)
      val tableNameRegex = toJavaRegex(tableNamePattern)

      val columns = tableEnv.getCatalog(catalogName).asScala.toArray.flatMap { flinkCatalog =>
        SchemaHelper.getSchemasWithPattern(flinkCatalog, schemaNameRegex)
          .flatMap { schemaName =>
            SchemaHelper.getFlinkTablesWithPattern(
              flinkCatalog,
              catalogName,
              schemaName,
              tableNameRegex)
              .filter { _._2.isDefined }
              .flatMap { case (tableName, flinkTable) =>
                val resolvedSchema = flinkTable.get.getUnresolvedSchema.resolve(resolver)
                val uniqueConstraint = resolvedSchema.getPrimaryKey.orElse(
                  UniqueConstraint.primaryKey("null_pri", Collections.emptyList()))
                uniqueConstraint
                  .getColumns.asScala.toArray.zipWithIndex
                  .map { case (column, pos) =>
                    toColumnResult(
                      catalogName,
                      schemaName,
                      tableName,
                      uniqueConstraint.getName,
                      column,
                      pos)
                  }
              }
          }
      }

      resultSet = ResultSet.builder.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical(TABLE_CAT, DataTypes.STRING),
          Column.physical(TABLE_SCHEM, DataTypes.STRING),
          Column.physical(TABLE_NAME, DataTypes.STRING),
          Column.physical(COLUMN_NAME, DataTypes.STRING),
          Column.physical(KEY_SEQ, DataTypes.INT),
          Column.physical(PK_NAME, DataTypes.STRING))
        .data(columns)
        .build
    } catch onError()
  }

  private def toColumnResult(
      catalogName: String,
      schemaName: String,
      tableName: String,
      pkName: String,
      columnName: String,
      pos: Int): Row = {
    // format: off
    Row.of(
      catalogName,                                                       // TABLE_CAT
      schemaName,                                                        // TABLE_SCHEM
      tableName,                                                         // TABLE_NAME
      columnName,                                                        // COLUMN_NAME
      Integer.valueOf(pos + 1),                                              // KEY_SEQ
      pkName                                                             // PK_NAME
    )
    // format: on
  }

}
