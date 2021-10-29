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

package org.apache.kyuubi.client

import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TProtocol

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.util.ThriftUtils

class KyuubiSyncThriftClient(protocol: TProtocol)
  extends TCLIService.Client(protocol) with Logging {

  @volatile private var _remoteSessionHandle: TSessionHandle = _

  private val lock = new ReentrantLock()

  /**
   * Lock every rpc call to send them sequentially
   */
  private def withLockAcquired[T](block: => T): T = {
    try {
      lock.lock()
      if (!protocol.getTransport.isOpen) {
        throw KyuubiSQLException.connectionDoesNotExist()
      }
      block
    } finally lock.unlock()
  }

  /**
   * Return the engine SessionHandle for kyuubi session so that we can get the same session id
   */
  def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      configs: Map[String, String]): SessionHandle = {
    val req = new TOpenSessionReq(protocol)
    req.setUsername(user)
    req.setPassword(password)
    req.setConfiguration(configs.asJava)
    val resp = withLockAcquired(OpenSession(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    _remoteSessionHandle = resp.getSessionHandle
    SessionHandle(_remoteSessionHandle, protocol)
  }

  def closeSession(): Unit = {
    val req = new TCloseSessionReq(_remoteSessionHandle)
    val resp = withLockAcquired(CloseSession(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
  }

  def executeStatement(
      statement: String,
      shouldRunAsync: Boolean,
      queryTimeout: Long): TOperationHandle = {
    val req = new TExecuteStatementReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setStatement(statement)
    req.setRunAsync(shouldRunAsync)
    req.setQueryTimeout(queryTimeout)
    val resp = withLockAcquired(ExecuteStatement(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getTypeInfo: TOperationHandle = {
    val req = new TGetTypeInfoReq(_remoteSessionHandle)
    val resp = withLockAcquired(GetTypeInfo(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getCatalogs: TOperationHandle = {
    val req = new TGetCatalogsReq(_remoteSessionHandle)
    val resp = withLockAcquired(GetCatalogs(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getSchemas(catalogName: String, schemaName: String): TOperationHandle = {
    val req = new TGetSchemasReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    val resp = withLockAcquired(GetSchemas(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): TOperationHandle = {
    val req = new TGetTablesReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    req.setTableName(tableName)
    req.setTableTypes(tableTypes)
    val resp = withLockAcquired(GetTables(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getTableTypes: TOperationHandle = {
    val req = new TGetTableTypesReq(_remoteSessionHandle)
    val resp = withLockAcquired(GetTableTypes(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getColumns(
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): TOperationHandle = {
    val req = new TGetColumnsReq(_remoteSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    req.setTableName(tableName)
    req.setColumnName(columnName)
    val resp = withLockAcquired(GetColumns(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getFunctions(
      catalogName: String,
      schemaName: String,
      functionName: String): TOperationHandle = {
    val req = new TGetFunctionsReq(_remoteSessionHandle, functionName)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    val resp = withLockAcquired(GetFunctions(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    withLockAcquired {
      super.GetOperationStatus(req)
    }
  }

  def cancelOperation(operationHandle: TOperationHandle): Unit = {
    val req = new TCancelOperationReq(operationHandle)
    val resp = withLockAcquired(CancelOperation(req))
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      info(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
    }
  }

  def closeOperation(operationHandle: TOperationHandle): Unit = {
    val req = new TCloseOperationReq(operationHandle)
    val resp = withLockAcquired(CloseOperation(req))
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      info(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
    }
  }

  def getResultSetMetadata(operationHandle: TOperationHandle): TTableSchema = {
    val req = new TGetResultSetMetadataReq(operationHandle)
    val resp = withLockAcquired(GetResultSetMetadata(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getSchema
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    withLockAcquired(super.FetchResults(req))
  }

  def fetchResults(
      operationHandle: TOperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet = {
    val or = FetchOrientation.toTFetchOrientation(orientation)
    val req = new TFetchResultsReq(operationHandle, or, maxRows)
    val fetchType = if (fetchLog) 1.toShort else 0.toShort
    req.setFetchType(fetchType)
    val resp = FetchResults(req)
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getResults
  }

  def sendCredentials(encodedCredentials: String): Unit = {
    // We hacked `TCLIService.Iface.RenewDelegationToken` to transfer Credentials to Spark SQL
    // engine
    val req = new TRenewDelegationTokenReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setDelegationToken(encodedCredentials)
    val resp = withLockAcquired(RenewDelegationToken(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
  }
}
