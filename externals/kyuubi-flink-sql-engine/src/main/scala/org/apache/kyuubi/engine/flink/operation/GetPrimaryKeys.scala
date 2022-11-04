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
import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.catalog.Column
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetPrimaryKeys(
    session: Session,
    catalogNameOrEmpty: String,
    schemaNameOrEmpty: String,
    tableName: String)
  extends FlinkOperation(session) {

  override protected def runInternal(): Unit = {
    try {
      val tableEnv = sessionContext.getExecutionContext.getTableEnvironment

      val catalogName =
        if (StringUtils.isEmpty(catalogNameOrEmpty)) tableEnv.getCurrentCatalog
        else catalogNameOrEmpty

      val schemaName =
        if (StringUtils.isEmpty(schemaNameOrEmpty)) {
          if (catalogName != tableEnv.getCurrentCatalog) {
            tableEnv.getCatalog(catalogName).get().getDefaultDatabase
          } else {
            tableEnv.getCurrentDatabase
          }
        } else schemaNameOrEmpty

      val flinkTable = tableEnv.from(s"`$catalogName`.`$schemaName`.`$tableName`")

      val resolvedSchema = flinkTable.getResolvedSchema
      val primaryKeySchema = resolvedSchema.getPrimaryKey
      val columns = primaryKeySchema.asScala.map { uniqueConstraint =>
        uniqueConstraint
          .getColumns.asScala.toArray.zipWithIndex
          .map { case (columnName, pos) =>
            toColumnResult(
              catalogName,
              schemaName,
              tableName,
              columnName,
              pos,
              uniqueConstraint.getName)
          }
      }.getOrElse(Array.empty)

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
      columnName: String,
      pos: Int,
      pkConstraintName: String): Row = {
    // format: off
    Row.of(
      catalogName,              // TABLE_CAT
      schemaName,               // TABLE_SCHEM
      tableName,                // TABLE_NAME
      columnName,               // COLUMN_NAME
      Integer.valueOf(pos + 1), // KEY_SEQ
      pkConstraintName          // PK_NAME
    )
    // format: on
  }

}
