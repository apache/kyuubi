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
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.catalog.ObjectIdentifier
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetTables(
    session: Session,
    catalog: String,
    schema: String,
    tableName: String,
    tableTypes: Set[String])
  extends FlinkOperation(session) {

  override protected def runInternal(): Unit = {
    try {
      val tableEnv = sessionContext.getExecutionContext.getTableEnvironment

      val catalogName = if (StringUtils.isEmpty(catalog)) tableEnv.getCurrentCatalog else catalog

      val schemaPattern = toJavaRegex(schema).r
      val tableNamePattern = toJavaRegex(tableName).r

      val tables = tableEnv.getCatalog(catalogName).asScala.toSeq.flatMap { flinkCatalog =>
        flinkCatalog.listDatabases().asScala
          .filter { _schema => schemaPattern.pattern.matcher(_schema).matches() }
          .flatMap { _schema =>
            flinkCatalog.listTables(_schema).asScala
              .filter { _table => tableNamePattern.pattern.matcher(_table).matches() }
              .map { _table =>
                val objPath = ObjectIdentifier.of(catalogName, _schema, _table).toObjectPath
                Try(flinkCatalog.getTable(objPath)) match {
                  case Success(table) => (_table, Some(table))
                  case Failure(_) => (_table, None)
                }
              }
              .filter(_._2.isDefined)
              .filter { _table =>
                tableTypes.contains(_table._2.get.getTableKind.name)
              }.map { _table =>
                Row.of(
                  catalogName,
                  _schema,
                  _table._1,
                  _table._2.get.getTableKind.name(),
                  _table._2.get.getComment,
                  null,
                  null,
                  null,
                  null,
                  null)
              }
          }
      }.toArray

      resultSet = ResultSet.builder.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical(TABLE_CAT, DataTypes.STRING()),
          Column.physical(TABLE_SCHEM, DataTypes.STRING()),
          Column.physical(TABLE_NAME, DataTypes.STRING()),
          Column.physical(TABLE_TYPE, DataTypes.STRING()),
          Column.physical(REMARKS, DataTypes.STRING()),
          Column.physical("TYPE_CAT", DataTypes.STRING()),
          Column.physical("TYPE_SCHEM", DataTypes.STRING()),
          Column.physical("TYPE_NAME", DataTypes.STRING()),
          Column.physical("SELF_REFERENCING_COL_NAME", DataTypes.STRING()),
          Column.physical("REF_GENERATION", DataTypes.STRING()))
        .data(tables)
        .build
    } catch onError()
  }
}
