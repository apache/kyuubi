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

package org.apache.kyuubi.server.trino.api

import scala.collection.JavaConverters._

import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.sql.parser.trino.KyuubiTrinoFeParser
import org.apache.kyuubi.sql.plan.PassThroughNode
import org.apache.kyuubi.sql.plan.trino.{GetCatalogs, GetColumns, GetPrimaryKeys, GetSchemas, GetTables, GetTableTypes, GetTypeInfo}

class KyuubiTrinoOperationTranslator(backendService: BackendService) {
  lazy val parser = new KyuubiTrinoFeParser()

  def transform(
      statement: String,
      sessionHandle: SessionHandle,
      configs: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = {
    parser.parsePlan(statement) match {
      case GetSchemas(catalogName, schemaPattern) =>
        backendService.getSchemas(sessionHandle, catalogName, schemaPattern)
      case GetCatalogs() =>
        backendService.getCatalogs(sessionHandle)
      case GetTableTypes() =>
        backendService.getTableTypes(sessionHandle)
      case GetTypeInfo() =>
        backendService.getTypeInfo(sessionHandle)
      case GetTables(catalogName, schemaPattern, tableNamePattern, tableTypes, emptyResult) =>
        val operationHandle = backendService.getTables(
          sessionHandle,
          catalogName,
          schemaPattern,
          tableNamePattern,
          tableTypes.asJava)
        operationHandle.setHasResultSet(!emptyResult)
        operationHandle
      case GetColumns(catalogName, schemaPattern, tableNamePattern, colNamePattern) =>
        backendService.getColumns(
          sessionHandle,
          catalogName,
          schemaPattern,
          tableNamePattern,
          colNamePattern)
      case GetPrimaryKeys() =>
        val operationHandle = backendService.getPrimaryKeys(sessionHandle, null, null, null)
        // The trino implementation always returns empty.
        operationHandle.setHasResultSet(false)
        operationHandle
      case PassThroughNode() =>
        backendService.executeStatement(sessionHandle, statement, configs, runAsync, queryTimeout)
    }
  }
}
