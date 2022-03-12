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

package org.apache.kyuubi.operation

import org.apache.hive.service.rpc.thrift.TRowSet

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.MetricsConstants.OPERATION_OPEN
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.{KyuubiSessionImpl, Session}
import org.apache.kyuubi.util.ThriftUtils

class KyuubiOperationManager private (name: String) extends OperationManager(name) {

  def this() = this(classOf[KyuubiOperationManager].getSimpleName)

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
  }


  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val operation =
      new ExecuteStatement(session, statement, confOverlay, runAsync, queryTimeout)
    addOperation(operation)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = {
    val operation = new GetTypeInfo(session)
    addOperation(operation)
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val operation = new GetCatalogs(session)
    addOperation(operation)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val operation = new GetSchemas(session, catalog, schema)
    addOperation(operation)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): Operation = {
    val operation = new GetTables(session, catalogName, schemaName, tableName, tableTypes)
    addOperation(operation)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val operation = new GetTableTypes(session)
    addOperation(operation)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val operation = new GetColumns(session, catalogName, schemaName, tableName, columnName)
    addOperation(operation)
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val operation = new GetFunctions(session, catalogName, schemaName, functionName)
    addOperation(operation)
  }

  def newLaunchEngineOperation(session: KyuubiSessionImpl, shouldRunAsync: Boolean): Operation = {
    val operation = new LaunchEngine(session, shouldRunAsync)
    addOperation(operation)
  }

  override def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TRowSet = {

    val operation = getOperation(opHandle).asInstanceOf[KyuubiOperation]
    val operationLog = operation.getOperationLog
    operationLog match {
      case Some(log) => log.read(maxRows)
      case None =>
        val remoteHandle = operation.remoteOpHandle()
        val client = operation.client
        if (remoteHandle != null) {
          client.fetchResults(remoteHandle, order, maxRows, fetchLog = true)
        } else {
          ThriftUtils.EMPTY_ROW_SET
        }
    }
  }

  override def start(): Unit = synchronized {
    MetricsSystem.tracing(_.registerGauge(OPERATION_OPEN, getOperationCount, 0))
    super.start()
  }
}
