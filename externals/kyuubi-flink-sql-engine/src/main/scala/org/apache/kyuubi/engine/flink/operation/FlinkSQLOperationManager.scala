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

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.flink.context.SessionContext
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.{Session, SessionHandle}

class FlinkSQLOperationManager extends OperationManager("FlinkSQLOperationManager") {

  private val sessionToFlink = new ConcurrentHashMap[SessionHandle, SessionContext]()

  def getFlinkSession(sessionHandle: SessionHandle): SessionContext = {
    val flinkSession = sessionToFlink.get(sessionHandle)
    if (flinkSession == null) {
      throw KyuubiSQLException(s"$sessionHandle has not been initialized or already been closed")
    }
    flinkSession
  }

  def setFlinkSession(sessionHandle: SessionHandle, sessionContext: SessionContext): Unit = {
    sessionToFlink.put(sessionHandle, sessionContext)
  }

  def removeFlinkSession(sessionHandle: SessionHandle): SessionContext = {
    logger.info(sessionToFlink.toString)
    sessionToFlink.remove(sessionHandle)
  }

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val op = new ExecuteStatement(
      getFlinkSession(session.handle),
      session,
      statement,
      runAsync,
      queryTimeout)
    addOperation(op)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = null

  override def newGetCatalogsOperation(session: Session): Operation = {
    val sessionContext = getFlinkSession(session.handle)
    val op = new GetCatalogs(sessionContext, session)
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

  override def newGetTableTypesOperation(session: Session): Operation = {
    val sessionContext = getFlinkSession(session.handle)
    val op = new GetTableTypes(sessionContext, session)
    addOperation(op)
  }

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
