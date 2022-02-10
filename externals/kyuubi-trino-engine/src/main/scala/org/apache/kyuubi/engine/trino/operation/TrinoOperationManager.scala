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

package org.apache.kyuubi.engine.trino.operation

import java.util

import org.apache.kyuubi.config.KyuubiConf.OPERATION_INCREMENTAL_COLLECT
import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.Session

class TrinoOperationManager extends OperationManager("TrinoOperationManager") {

  def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val incrementalCollect = session.sessionManager.getConf.get(OPERATION_INCREMENTAL_COLLECT)
    val operation = new ExecuteStatement(session, statement, runAsync, incrementalCollect)
    addOperation(operation)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = null

  override def newGetCatalogsOperation(session: Session): Operation = {
    val op = new GetCatalogs(session)
    addOperation(op)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = null

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = null

  override def newGetTableTypesOperation(session: Session): Operation = null

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = null

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = null
}
