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

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.TException
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.transport.TSocket

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_LOGIN_TIMEOUT, ENGINE_REQUEST_TIMEOUT}
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.service.TFrontendService.DELEGATION_TOKEN_IS_NOT_SUPPORTED
import org.apache.kyuubi.service.authentication.PlainSASLHelper
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.util.{ThreadUtils, ThriftUtils}

class KyuubiSyncThriftClient private (
    protocol: TProtocol,
    maxAttempts: Int,
    engineAliveProbeProtocol: Option[TProtocol],
    engineAliveProbeInterval: Long,
    engineAliveTimeout: Long)
  extends TCLIService.Client(protocol) with Logging {

  @volatile private var _remoteSessionHandle: TSessionHandle = _
  @volatile private var _engineId: Option[String] = _

  private val lock = new ReentrantLock()

  @volatile private var _aliveProbeSessionHandle: TSessionHandle = _
  @volatile private var remoteEngineBroken: Boolean = false
  private val engineAliveProbeClient = engineAliveProbeProtocol.map(new TCLIService.Client(_))
  private var engineAliveThreadPool: ScheduledExecutorService = _
  private var engineLastAlive: Long = _

  private def startEngineAliveProbe(): Unit = {
    engineAliveThreadPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "engine-alive-probe-" + _aliveProbeSessionHandle)
    val task = new Runnable {
      override def run(): Unit = {
        if (!remoteEngineBroken) {
          engineAliveProbeClient.foreach { client =>
            val tGetInfoReq = new TGetInfoReq()
            tGetInfoReq.setSessionHandle(_aliveProbeSessionHandle)
            tGetInfoReq.setInfoType(TGetInfoType.CLI_DBMS_VER)

            try {
              client.GetInfo(tGetInfoReq).getInfoValue.getStringValue
              engineLastAlive = System.currentTimeMillis()
              remoteEngineBroken = false
            } catch {
              case e: Throwable =>
                warn(s"The engine alive probe fails", e)
                val now = System.currentTimeMillis()
                if (now - engineLastAlive > engineAliveTimeout) {
                  error("Mark the engine not alive with no recent alive probe success:" +
                    s" ${now - engineLastAlive} ms exceeds timeout $engineAliveTimeout ms")
                  remoteEngineBroken = true
                }
            }
          }
        }
      }
    }
    engineLastAlive = System.currentTimeMillis()
    engineAliveThreadPool.scheduleAtFixedRate(
      task,
      engineAliveProbeInterval,
      engineAliveProbeInterval,
      TimeUnit.MILLISECONDS)
  }

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

  private def withRetryingRequest[T](block: => T, request: String): T = withLockAcquired {
    var attemptCount = 1

    var resp: T = null.asInstanceOf[T]
    while (attemptCount <= maxAttempts && resp == null) {
      try {
        resp = block
        remoteEngineBroken = false
      } catch {
        case e: TException if attemptCount < maxAttempts && isConnectionValid() =>
          warn(s"Failed to execute $request after $attemptCount/$maxAttempts times, retrying", e)
          attemptCount += 1
          Thread.sleep(100)
        case e: Throwable =>
          error(s"Failed to execute $request after $attemptCount/$maxAttempts times, aborting", e)
          throw e
      }
    }
    resp
  }

  def engineId: Option[String] = _engineId

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
    val resp = withRetryingRequest(OpenSession(req), "OpenSession")
    ThriftUtils.verifyTStatus(resp.getStatus)
    _remoteSessionHandle = resp.getSessionHandle
    _engineId = Option(resp.getConfiguration)
      .filter(_.containsKey("kyuubi.engine.id"))
      .map(_.get("kyuubi.engine.id"))

    engineAliveProbeClient.foreach { aliveProbeClient =>
      Utils.tryLogNonFatalError {
        val resp = aliveProbeClient.OpenSession(req)
        ThriftUtils.verifyTStatus(resp.getStatus)
        _aliveProbeSessionHandle = resp.getSessionHandle
        startEngineAliveProbe()
      }
    }

    SessionHandle(_remoteSessionHandle, protocol)
  }

  def closeSession(): Unit = {
    try {
      if (_remoteSessionHandle != null) {
        val req = new TCloseSessionReq(_remoteSessionHandle)
        val resp = withRetryingRequest(CloseSession(req), "CloseSession")
        ThriftUtils.verifyTStatus(resp.getStatus)
      }
    } catch {
      case e: Exception =>
        throw KyuubiSQLException("Error while cleaning up the engine resources", e)
    } finally {
      Option(engineAliveThreadPool).foreach(_.shutdown())
      if (_aliveProbeSessionHandle != null && !remoteEngineBroken) {
        engineAliveProbeClient.foreach { client =>
          Utils.tryLogNonFatalError {
            val req = new TCloseSessionReq(_aliveProbeSessionHandle)
            val resp = client.CloseSession(req)
            ThriftUtils.verifyTStatus(resp.getStatus)
          }
        }
      }
      Seq(protocol).union(engineAliveProbeProtocol.toSeq).foreach { tProtocol =>
        if (tProtocol.getTransport.isOpen) tProtocol.getTransport.close()
      }
    }
  }

  def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      shouldRunAsync: Boolean,
      queryTimeout: Long): TOperationHandle = {
    val req = new TExecuteStatementReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setStatement(statement)
    req.setConfOverlay(confOverlay.asJava)
    req.setRunAsync(shouldRunAsync)
    req.setQueryTimeout(queryTimeout)
    val resp = withRetryingRequest(ExecuteStatement(req), "ExecuteStatement")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getTypeInfo: TOperationHandle = {
    val req = new TGetTypeInfoReq(_remoteSessionHandle)
    val resp = withRetryingRequest(GetTypeInfo(req), "GetTypeInfo")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getCatalogs: TOperationHandle = {
    val req = new TGetCatalogsReq(_remoteSessionHandle)
    val resp = withRetryingRequest(GetCatalogs(req), "GetCatalogs")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getSchemas(catalogName: String, schemaName: String): TOperationHandle = {
    val req = new TGetSchemasReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    val resp = withRetryingRequest(GetSchemas(req), "GetSchemas")
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
    val resp = withRetryingRequest(GetTables(req), "GetTables")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getTableTypes: TOperationHandle = {
    val req = new TGetTableTypesReq(_remoteSessionHandle)
    val resp = withRetryingRequest(GetTableTypes(req), "GetTableTypes")
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
    val resp = withRetryingRequest(GetColumns(req), "GetColumns")
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
    val resp = withRetryingRequest(GetFunctions(req), "GetFunctions")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getPrimaryKeys(
      catalogName: String,
      schemaName: String,
      tableName: String): TOperationHandle = {
    val req = new TGetPrimaryKeysReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    req.setTableName(tableName)
    val resp = withRetryingRequest(GetPrimaryKeys(req), "GetPrimaryKeys")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getCrossReference(
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): TOperationHandle = {
    val req = new TGetCrossReferenceReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setParentCatalogName(primaryCatalog)
    req.setParentSchemaName(primarySchema)
    req.setParentTableName(primaryTable)
    req.setForeignCatalogName(foreignCatalog)
    req.setForeignSchemaName(foreignSchema)
    req.setForeignTableName(foreignTable)
    val resp = withRetryingRequest(GetCrossReference(req), "GetCrossReference")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getOperationStatus(operationHandle: TOperationHandle): TGetOperationStatusResp = {
    val req = new TGetOperationStatusReq(operationHandle)
    val resp = withRetryingRequest(GetOperationStatus(req), "GetOperationStatus")
    resp
  }

  def cancelOperation(operationHandle: TOperationHandle): Unit = {
    val req = new TCancelOperationReq(operationHandle)
    val resp = withRetryingRequest(CancelOperation(req), "CancelOperation")
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      info(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
    }
  }

  def closeOperation(operationHandle: TOperationHandle): Unit = {
    val req = new TCloseOperationReq(operationHandle)
    val resp = withRetryingRequest(CloseOperation(req), "CloseOperation")
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      info(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
    }
  }

  def getResultSetMetadata(operationHandle: TOperationHandle): TTableSchema = {
    val req = new TGetResultSetMetadataReq(operationHandle)
    val resp = withRetryingRequest(GetResultSetMetadata(req), "GetResultSetMetadata")
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getSchema
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
    val resp = withRetryingRequest(FetchResults(req), "FetchResults")
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
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      debug(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
      if(DELEGATION_TOKEN_IS_NOT_SUPPORTED.equals(resp.getStatus.getErrorMessage)) {
        throw KyuubiSQLException(resp.getStatus.getErrorMessage)
      }
    }

  }

  def isConnectionValid(): Boolean = {
    !remoteEngineBroken && protocol.getTransport.isOpen
  }
}

private[kyuubi] object KyuubiSyncThriftClient {
  private def createTProtocol(
      user: String,
      passwd: String,
      host: String,
      port: Int,
      socketTimeout: Int,
      connectionTimeout: Int): TProtocol = {
    val tSocket = new TSocket(host, port, socketTimeout, connectionTimeout)
    val tTransport = PlainSASLHelper.getPlainTransport(user, passwd, tSocket)
    tTransport.open()
    new TBinaryProtocol(tTransport)
  }

  def createClient(
      user: String,
      password: String,
      host: String,
      port: Int,
      conf: KyuubiConf): KyuubiSyncThriftClient = {
    val passwd = Option(password).filter(_.nonEmpty).getOrElse("anonymous")
    val loginTimeout = conf.get(ENGINE_LOGIN_TIMEOUT).toInt
    val requestTimeout = conf.get(ENGINE_REQUEST_TIMEOUT).toInt
    val requestMaxAttempts = conf.get(KyuubiConf.OPERATION_THRIFT_CLIENT_REQUEST_MAX_ATTEMPTS)
    val aliveProbeEnabled = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED)
    val aliveProbeInterval = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL).toInt
    val aliveTimeout = conf.get(KyuubiConf.ENGINE_ALIVE_TIMEOUT)

    val tProtocol = createTProtocol(user, passwd, host, port, requestTimeout, loginTimeout)
    val aliveProbeProtocol =
      if (aliveProbeEnabled) {
        Option(createTProtocol(user, passwd, host, port, aliveProbeInterval, loginTimeout))
      } else {
        None
      }
    new KyuubiSyncThriftClient(
      tProtocol,
      requestMaxAttempts,
      aliveProbeProtocol,
      aliveProbeInterval,
      aliveTimeout)
  }
}
