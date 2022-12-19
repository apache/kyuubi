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

import java.util.concurrent.{ExecutorService, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionException
import scala.concurrent.duration.Duration

import com.google.common.annotations.VisibleForTesting
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.transport.TSocket

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_LOGIN_TIMEOUT
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.service.authentication.PlainSASLHelper
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.util.{ThreadUtils, ThriftUtils}

class KyuubiSyncThriftClient private (
    protocol: TProtocol,
    engineAliveProbeProtocol: Option[TProtocol],
    engineAliveProbeInterval: Long,
    engineAliveTimeout: Long)
  extends TCLIService.Client(protocol) with Logging {

  @volatile private var _remoteSessionHandle: TSessionHandle = _
  @volatile private var _engineId: Option[String] = _
  @volatile private var _engineUrl: Option[String] = _
  @volatile private var _engineName: Option[String] = _

  private val lock = new ReentrantLock()

  @volatile private var _aliveProbeSessionHandle: TSessionHandle = _
  @volatile private var remoteEngineBroken: Boolean = false
  private val engineAliveProbeClient = engineAliveProbeProtocol.map(new TCLIService.Client(_))
  private var engineAliveThreadPool: ScheduledExecutorService = _
  @volatile private var engineLastAlive: Long = _

  private var asyncRequestExecutor: ExecutorService = _

  @VisibleForTesting
  @volatile private[kyuubi] var asyncRequestInterrupted: Boolean = false

  @VisibleForTesting
  private[kyuubi] def getEngineAliveProbeProtocol: Option[TProtocol] = engineAliveProbeProtocol

  private def newAsyncRequestExecutor(): ExecutorService = {
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "async-request-executor-" + _remoteSessionHandle)
  }

  private def shutdownAsyncRequestExecutor(): Unit = {
    Option(asyncRequestExecutor).filterNot(_.isShutdown).foreach(ThreadUtils.shutdown(_))
    asyncRequestInterrupted = true
  }

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
                warn(s"The engine[$engineId] alive probe fails", e)
                val now = System.currentTimeMillis()
                if (now - engineLastAlive > engineAliveTimeout) {
                  error(s"Mark the engine[$engineId] not alive with no recent alive probe" +
                    s" success: ${now - engineLastAlive} ms exceeds timeout $engineAliveTimeout ms")
                  remoteEngineBroken = true
                }
            }
          }
        } else {
          shutdownAsyncRequestExecutor()
        }
      }
    }
    engineLastAlive = System.currentTimeMillis()
    engineAliveThreadPool.scheduleWithFixedDelay(
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

  private def withLockAcquiredAsyncRequest[T](block: => T): T = withLockAcquired {
    if (asyncRequestExecutor == null || asyncRequestExecutor.isShutdown) {
      asyncRequestExecutor = newAsyncRequestExecutor()
    }

    val task = asyncRequestExecutor.submit(() => {
      val resp = block
      remoteEngineBroken = false
      resp
    })

    try {
      task.get()
    } catch {
      case e: ExecutionException => throw e.getCause
      case e: Throwable => throw e
    }
  }

  def engineId: Option[String] = _engineId
  def engineName: Option[String] = _engineName
  def engineUrl: Option[String] = _engineUrl

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
    _engineId = Option(resp.getConfiguration)
      .filter(_.containsKey(KYUUBI_ENGINE_ID))
      .map(_.get(KYUUBI_ENGINE_ID))
    _engineName = Option(resp.getConfiguration)
      .filter(_.containsKey(KYUUBI_ENGINE_NAME))
      .map(_.get(KYUUBI_ENGINE_NAME))
    _engineUrl = Option(resp.getConfiguration)
      .filter(_.containsKey(KYUUBI_ENGINE_URL))
      .map(_.get(KYUUBI_ENGINE_URL))

    engineAliveProbeClient.foreach { aliveProbeClient =>
      val sessionName = SessionHandle.apply(_remoteSessionHandle).identifier + "_aliveness_probe"
      Utils.tryLogNonFatalError {
        req.setConfiguration((configs ++ Map(KyuubiConf.SESSION_NAME.key -> sessionName)).asJava)
        val resp = aliveProbeClient.OpenSession(req)
        ThriftUtils.verifyTStatus(resp.getStatus)
        _aliveProbeSessionHandle = resp.getSessionHandle
        startEngineAliveProbe()
      }
    }

    SessionHandle(_remoteSessionHandle)
  }

  def closeSession(): Unit = {
    try {
      if (_remoteSessionHandle != null) {
        val req = new TCloseSessionReq(_remoteSessionHandle)
        val resp = withLockAcquiredAsyncRequest(CloseSession(req))
        ThriftUtils.verifyTStatus(resp.getStatus)
      }
    } catch {
      case e: Exception =>
        throw KyuubiSQLException("Error while cleaning up the engine resources", e)
    } finally {
      Option(engineAliveThreadPool).foreach { pool =>
        ThreadUtils.shutdown(pool, Duration(engineAliveProbeInterval, TimeUnit.MILLISECONDS))
      }
      if (_aliveProbeSessionHandle != null) {
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
      shutdownAsyncRequestExecutor()
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
    val resp = withLockAcquiredAsyncRequest(ExecuteStatement(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getInfo(infoType: TGetInfoType): TGetInfoResp = {
    val req = new TGetInfoReq(_remoteSessionHandle, infoType)
    val resp = withLockAcquiredAsyncRequest(GetInfo(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp
  }

  def getTypeInfo: TOperationHandle = {
    val req = new TGetTypeInfoReq(_remoteSessionHandle)
    val resp = withLockAcquiredAsyncRequest(GetTypeInfo(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getCatalogs: TOperationHandle = {
    val req = new TGetCatalogsReq(_remoteSessionHandle)
    val resp = withLockAcquiredAsyncRequest(GetCatalogs(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getSchemas(catalogName: String, schemaName: String): TOperationHandle = {
    val req = new TGetSchemasReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setCatalogName(catalogName)
    req.setSchemaName(schemaName)
    val resp = withLockAcquiredAsyncRequest(GetSchemas(req))
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
    val resp = withLockAcquiredAsyncRequest(GetTables(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getTableTypes: TOperationHandle = {
    val req = new TGetTableTypesReq(_remoteSessionHandle)
    val resp = withLockAcquiredAsyncRequest(GetTableTypes(req))
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
    val resp = withLockAcquiredAsyncRequest(GetColumns(req))
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
    val resp = withLockAcquiredAsyncRequest(GetFunctions(req))
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
    val resp = withLockAcquiredAsyncRequest(GetPrimaryKeys(req))
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
    val resp = withLockAcquiredAsyncRequest(GetCrossReference(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getOperationHandle
  }

  def getQueryId(operationHandle: TOperationHandle): TGetQueryIdResp = {
    val req = new TGetQueryIdReq(operationHandle)
    val resp = withLockAcquiredAsyncRequest(GetQueryId(req))
    resp
  }

  def getOperationStatus(operationHandle: TOperationHandle): TGetOperationStatusResp = {
    val req = new TGetOperationStatusReq(operationHandle)
    val resp = withLockAcquiredAsyncRequest(GetOperationStatus(req))
    resp
  }

  def cancelOperation(operationHandle: TOperationHandle): Unit = {
    val req = new TCancelOperationReq(operationHandle)
    val resp = withLockAcquiredAsyncRequest(CancelOperation(req))
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      info(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
    }
  }

  def closeOperation(operationHandle: TOperationHandle): Unit = {
    val req = new TCloseOperationReq(operationHandle)
    val resp = withLockAcquiredAsyncRequest(CloseOperation(req))
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      info(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
    }
  }

  def getResultSetMetadata(operationHandle: TOperationHandle): TGetResultSetMetadataResp = {
    val req = new TGetResultSetMetadataReq(operationHandle)
    val resp = withLockAcquiredAsyncRequest(GetResultSetMetadata(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp
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
    val resp = withLockAcquiredAsyncRequest(FetchResults(req))
    ThriftUtils.verifyTStatus(resp.getStatus)
    resp.getResults
  }

  def sendCredentials(encodedCredentials: String): Unit = {
    // We hacked `TCLIService.Iface.RenewDelegationToken` to transfer Credentials to Spark SQL
    // engine
    val req = new TRenewDelegationTokenReq()
    req.setSessionHandle(_remoteSessionHandle)
    req.setDelegationToken(encodedCredentials)
    try {
      val resp = withLockAcquiredAsyncRequest(RenewDelegationToken(req))
      if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
        debug(s"$req succeed on engine side")
      } else {
        warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
      }
    } catch {
      case e: Exception => warn(s"$req failed on engine side", e)
    }
  }
}

private[kyuubi] object KyuubiSyncThriftClient extends Logging {

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
    val aliveProbeEnabled = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED)
    val aliveProbeInterval = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL).toInt
    val aliveTimeout = conf.get(KyuubiConf.ENGINE_ALIVE_TIMEOUT)

    val tProtocol = createTProtocol(user, passwd, host, port, 0, loginTimeout)

    val aliveProbeProtocol =
      if (aliveProbeEnabled) {
        Option(createTProtocol(user, passwd, host, port, aliveProbeInterval, loginTimeout))
      } else {
        None
      }
    new KyuubiSyncThriftClient(
      tProtocol,
      aliveProbeProtocol,
      aliveProbeInterval,
      aliveTimeout)
  }
}
