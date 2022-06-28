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
import org.apache.flink.table.catalog.ObjectIdentifier

import org.apache.kyuubi.engine.flink.result.{Constants, ResultSetUtil}
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
              .filter { _table =>
                // skip check type of every table if request all types
                if (Set(Constants.TABLE_TYPE, Constants.VIEW_TYPE) subsetOf tableTypes) {
                  true
                } else {
                  val objPath = ObjectIdentifier.of(catalogName, _schema, _table).toObjectPath
                  Try(flinkCatalog.getTable(objPath)) match {
                    case Success(table) => tableTypes.contains(table.getTableKind.name)
                    case Failure(_) => false
                  }
                }
              }
          }
      }

      resultSet = ResultSetUtil.stringListToResultSet(tables.toList, Constants.SHOW_TABLES_RESULT)
    } catch onError()
  }
}
