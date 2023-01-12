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

import java.util.concurrent.TimeUnit

import org.apache.hive.service.rpc.thrift.TRowSet

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.OPERATION_QUERY_TIMEOUT
import org.apache.kyuubi.metrics.MetricsConstants.OPERATION_OPEN
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionImpl, Session}
import org.apache.kyuubi.sql.plan.command.RunnableCommand
import org.apache.kyuubi.util.ThriftUtils

class KyuubiOperationManager private (name: String) extends OperationManager(name) {

  def this() = this(classOf[KyuubiOperationManager].getSimpleName)

  private var queryTimeout: Option[Long] = None

  override def initialize(conf: KyuubiConf): Unit = {
    queryTimeout = conf.get(OPERATION_QUERY_TIMEOUT).map(TimeUnit.MILLISECONDS.toSeconds)
    super.initialize(conf)
  }

  private def getQueryTimeout(clientQueryTimeout: Long): Long = {
    // If clientQueryTimeout is smaller than systemQueryTimeout value,
    // we use the clientQueryTimeout value.
    queryTimeout match {
      case Some(systemQueryTimeout) if clientQueryTimeout > 0 =>
        math.min(systemQueryTimeout, clientQueryTimeout)
      case Some(systemQueryTimeout) => systemQueryTimeout
      case None => clientQueryTimeout
    }
  }

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val operation =
      new ExecuteStatement(session, statement, confOverlay, runAsync, getQueryTimeout(queryTimeout))
    addOperation(operation)
  }

  def newExecuteOnServerOperation(
      session: KyuubiSessionImpl,
      runAsync: Boolean,
      command: RunnableCommand): Operation = {
    val operation = new ExecutedCommandExec(session, runAsync, command)
    addOperation(operation)
  }

  def newBatchJobSubmissionOperation(
      session: KyuubiBatchSessionImpl,
      batchType: String,
      batchName: String,
      resource: String,
      className: String,
      batchConf: Map[String, String],
      batchArgs: Seq[String],
      recoveryMetadata: Option[Metadata]): BatchJobSubmission = {
    val operation = new BatchJobSubmission(
      session,
      batchType,
      batchName,
      resource,
      className,
      batchConf,
      batchArgs,
      recoveryMetadata)
    addOperation(operation)
    operation
  }

  // The server does not use these 4 operations
  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetCurrentCatalogOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetCurrentDatabaseOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
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

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = {
    val operation = new GetPrimaryKeys(session, catalogName, schemaName, tableName)
    addOperation(operation)
  }

  override def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation = {
    val operation = new GetCrossReference(
      session,
      primaryCatalog,
      primarySchema,
      primaryTable,
      foreignCatalog,
      foreignSchema,
      foreignTable)
    addOperation(operation)
  }

  override def getQueryId(operation: Operation): String = {
    val kyuubiOperation = operation.asInstanceOf[KyuubiOperation]
    val client = kyuubiOperation.client
    val remoteHandle = kyuubiOperation.remoteOpHandle()
    if (remoteHandle != null) {
      val queryId = client.getQueryId(remoteHandle).getQueryId
      queryId
    } else {
      null
    }
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
