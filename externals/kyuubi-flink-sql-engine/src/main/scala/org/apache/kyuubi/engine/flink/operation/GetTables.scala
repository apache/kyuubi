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

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}
import scala.util.{Failure, Success, Try}

import org.apache.flink.table.catalog.ObjectIdentifier

import org.apache.kyuubi.engine.flink.result.{Constants, OperationUtil}
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.session.Session

class GetTables(
    session: Session,
    catalog: String,
    schema: String,
    tableName: String,
    tableTypes: Set[String])
  extends FlinkOperation(OperationType.GET_TABLES, session) {

  override protected def runInternal(): Unit = {
    try {
      val tableEnv = sessionContext.getExecutionContext.getTableEnvironment

      var catalogName = catalog
      if (catalog == null || catalog.isEmpty) {
        catalogName = tableEnv.getCurrentCatalog
      }

      val schemaPattern = toJavaRegex(schema).r.pattern
      val tableNamePattern = toJavaRegex(tableName).r.pattern

      var tables = List[String]()

      val optional = tableEnv.getCatalog(catalogName)
      if (optional.isPresent) {
        val currCatalog = optional.get()
        tables = currCatalog.listDatabases().asScala
          .filter(database =>
            schemaPattern.matcher(database).matches())
          .flatMap { database =>
            currCatalog.listTables(database).asScala
              .filter(identifier =>
                tableNamePattern.matcher(identifier).matches())
              .filter(identifier => {
                // only table or view
                if (!tableTypes.contains(Constants.TABLE_TYPE) || !tableTypes.contains(
                    Constants.VIEW_TYPE)) {
                  // try to get table kind
                  Try(currCatalog.getTable(ObjectIdentifier.of(
                    catalogName,
                    database,
                    identifier).toObjectPath)) match {
                    case Success(table) => tableTypes.contains(table.getTableKind.name())
                    case Failure(_) => false
                  }
                } else {
                  true
                }
              })
          }.toList
      }

      resultSet = OperationUtil.stringListToResultSet(
        tables.asJava,
        Constants.SHOW_TABLES_RESULT)

    } catch onError()
  }
}
