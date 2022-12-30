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

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.sql.parser.trino.KyuubiTrinoFeParser
import org.apache.kyuubi.sql.plan.PassThroughNode
import org.apache.kyuubi.sql.plan.trino.{GetCatalogs, GetSchemas, GetTableTypes}

class KyuubiTrinoOperationTranslator(backendService: BackendService) {
  lazy val parser = new KyuubiTrinoFeParser()

  def transform(
      statement: String,
      user: String,
      ipAddress: String,
      configs: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = {
    val sessionHandle = backendService.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      user,
      "",
      ipAddress,
      configs)
    parser.parsePlan(statement) match {
      case GetSchemas(catalogName, schemaPattern) =>
        backendService.getSchemas(sessionHandle, catalogName, schemaPattern)
      case GetCatalogs() =>
        backendService.getCatalogs(sessionHandle)
      case GetTableTypes() =>
        backendService.getTableTypes(sessionHandle)
      case PassThroughNode() =>
        backendService.executeStatement(sessionHandle, statement, configs, runAsync, queryTimeout)
    }
  }
}
