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

import java.util.UUID
import java.util.concurrent.{ExecutorService, Future, ScheduledExecutorService, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionException
import scala.concurrent.duration.{Deadline, FiniteDuration}

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_ALIVE_PROBE_VIRTUAL_THREADS_ENABLED, ENGINE_CLEANUP_TIMEOUT, ENGINE_LOGIN_TIMEOUT, ENGINE_RPC_CLIENT_VIRTUAL_THREADS_ENABLED}
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.service.authentication.PlainSASLHelper
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.thrift.TConfiguration
import org.apache.kyuubi.shaded.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.kyuubi.shaded.thrift.transport.TSocket
import org.apache.kyuubi.util.{ThreadUtils, ThriftUtils}
import org.apache.kyuubi.util.ThreadUtils.scheduleTolerableRunnableWithFixedDelay

class KyuubiSyncThriftClient private (
    val hostPort: (String, Int),
    protocol: TProtocol,
    engineAliveProbeProtocol: Option[TProtocol],
    engineAliveProbeInterval: Long,
    engineAliveTimeout: Long,
    cleanupTimeout: Long,
    useVirtualThreadsForAliveProbe: Boolean,
    useVirtualThreadsForAsyncRequests: Boolean)
  extends TCLIService.Client(protocol) with Logging {

  @volatile private var _remoteSessionHandle: TSessionHandle = _
  @volatile private var _engineId: Option[String] = _
  @volatile private var _engineUrl: Option[String] = _
  @volatile private var _engineName: Option[String] = _

  private val clientClosed = new AtomicBoolean()
  private[kyuubi] def engineConnectionClosed: Boolean =
    clientClosed.get() || !protocol.getTransport.isOpen

  private val lock = new ReentrantLock()

  // Visible for testing.
  private[kyuubi] def remoteSessionHandle: TSessionHandle = _remoteSessionHandle

  @volatile private var _aliveProbeSessionHandle: TSessionHandle = _
  @volatile private var _remoteEngineBroken: Boolean = false
  private[kyuubi] def remoteEngineBroken: Boolean = _remoteEngineBroken
  @volatile private var clientClosedByAliveProbe: Boolean = false
  private val engineAliveProbeClient = engineAliveProbeProtocol.map(new TCLIService.Client(_))
  @volatile private var engineAliveThreadPool: ScheduledExecutorService = _
  @volatile private var engineLastAlive: Long = _

  @volatile private var asyncRequestExecutorInitialized: Boolean = false
  @volatile private var currentRequest: Future[_] = _
  private lazy val asyncRequestExecutor: ExecutorService = {
    asyncRequestExecutorInitialized = true
    val threadName = "async-request-executor-" + SessionHandle(_remoteSessionHandle)
    val executor = if (useVirtualThreadsForAsyncRequests) {
      // The session lock serializes submissions, so this is not a normal request backlog.
      // Retain the platform executor's effectively unbounded queue: a completed or cancelled
      // Future does not imply that the previous task has released its execution permit.
      ThreadUtils.newBoundedQueuedVirtualThreadPerTaskExecutor(1, Int.MaxValue - 1, threadName)
    } else {
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(threadName)
    }
    // Closing a client must also cover an executor initialized concurrently with that close.
    if (clientClosed.get()) executor.shutdownNow()
    executor
  }

  @VisibleForTesting
  @volatile private[kyuubi] var asyncRequestInterrupted: Boolean = false

  @VisibleForTesting
  private[kyuubi] def asyncRequestExecutorTerminated: Boolean =
    !asyncRequestExecutorInitialized || asyncRequestExecutor.isTerminated

  @VisibleForTesting
  private[kyuubi] def getEngineAliveProbeProtocol: Option[TProtocol] = engineAliveProbeProtocol

  private def closeClient(): Unit = {
    if (clientClosed.compareAndSet(false, true)) {
      // Do not acquire the client lock: its owner can be waiting for this socket's response.
      Seq(protocol).union(engineAliveProbeProtocol.toSeq).foreach { tProtocol =>
        Utils.tryLogNonFatalError {
          tProtocol.getTransport.close()
        }
      }
      Option(currentRequest).foreach(_.cancel(true))
      if (asyncRequestExecutorInitialized) asyncRequestExecutor.shutdownNow()
      asyncRequestInterrupted = true
      Option(engineAliveThreadPool).foreach(_.shutdownNow())
    }
  }

  private[kyuubi] def withCleanupRequest[T](operation: String)(f: Deadline => T): T = {
    val deadline = Deadline.now + FiniteDuration(cleanupTimeout, TimeUnit.MILLISECONDS)
    try f(deadline)
    catch {
      case e: TimeoutException =>
        closeClient()
        throw KyuubiSQLException(
          s"$operation timed out after $cleanupTimeout ms; the session's engine connection " +
            "is closed and engine-side cleanup is unconfirmed",
          e)
      case e: InterruptedException =>
        closeClient()
        Thread.currentThread().interrupt()
        throw KyuubiSQLException(
          s"$operation was interrupted; the session's engine connection is closed and " +
            "engine-side cleanup is unconfirmed",
          e)
    }
  }

  private def startEngineAliveProbe(): Unit = {
    val threadName = "engine-alive-probe-" + _aliveProbeSessionHandle
    engineAliveThreadPool = if (useVirtualThreadsForAliveProbe) {
      ThreadUtils.newVirtualThreadSingleThreadScheduledExecutor(threadName)
    } else {
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(threadName)
    }

    def closeClientByAliveProbe(): Unit = {
      warn(s"Removing Clients for ${_remoteSessionHandle}")
      clientClosedByAliveProbe = true
      closeClient()
    }

    val task = new Runnable {
      override def run(): Unit = {
        if (!_remoteEngineBroken && !engineConnectionClosed) {
          engineAliveProbeClient.foreach { client =>
            val tGetInfoReq = new TGetInfoReq()
            tGetInfoReq.setSessionHandle(_aliveProbeSessionHandle)
            tGetInfoReq.setInfoType(TGetInfoType.CLI_DBMS_VER)

            try {
              client.GetInfo(tGetInfoReq).getInfoValue.getStringValue
              engineLastAlive = System.currentTimeMillis()
              _remoteEngineBroken = false
            } catch {
              case e: Throwable =>
                val engineIdStr = engineId.getOrElse("")
                warn(s"The engine[$engineIdStr] alive probe fails", e)
                val now = System.currentTimeMillis()
                if (now - engineLastAlive > engineAliveTimeout) {
                  error(s"Mark the engine[$engineIdStr] not alive with no recent alive probe" +
                    s" success: ${now - engineLastAlive} ms exceeds timeout $engineAliveTimeout ms")
                  _remoteEngineBroken = true
                  closeClientByAliveProbe()
                }
            }
          }
        } else {
          closeClientByAliveProbe()
        }
      }
    }
    engineLastAlive = System.currentTimeMillis()
    scheduleTolerableRunnableWithFixedDelay(
      engineAliveThreadPool,
      task,
      engineAliveProbeInterval,
      engineAliveProbeInterval,
      TimeUnit.MILLISECONDS)
    if (clientClosed.get()) engineAliveThreadPool.shutdownNow()
  }

  /**
   * Lock every rpc call to send them sequentially
   */
  private def withLockAcquired[T](block: => T): T = Utils.withLockRequired(lock) {
    if (engineConnectionClosed) {
      throw KyuubiSQLException.connectionDoesNotExist()
    }
    block
  }

  private def withLockAcquiredAsyncRequest[T](
      block: => T,
      deadline: Option[Deadline] = None): T = {
    def request(): T = {
      if (engineConnectionClosed || asyncRequestExecutor.isShutdown) {
        throw KyuubiSQLException.connectionDoesNotExist()
      }
      if (deadline.exists(_.isOverdue())) {
        throw new TimeoutException("Timed out before submitting the cleanup RPC")
      }

      val task = asyncRequestExecutor.submit(() => {
        val resp = block
        _remoteEngineBroken = false
        resp
      })
      currentRequest = task
      if (clientClosed.get()) task.cancel(true)

      try {
        deadline match {
          case Some(limit) => task.get(limit.timeLeft.toNanos, TimeUnit.NANOSECONDS)
          case None => task.get()
        }
      } catch {
        case e: ExecutionException => throw e.getCause
      } finally {
        currentRequest = null
      }
    }

    deadline match {
      case Some(limit) =>
        if (limit.isOverdue() || !lock.tryLock(limit.timeLeft.toNanos, TimeUnit.NANOSECONDS)) {
          closeClient()
          throw new TimeoutException("Timed out waiting for the engine client lock")
        }
        try request()
        catch {
          // Mark the connection closed before unlocking, so no request can reuse a late response.
          case e @ (_: TimeoutException | _: InterruptedException) =>
            closeClient()
            throw e
        } finally lock.unlock()
      case None => withLockAcquired(request())
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
    req.setConfiguration((configs ++ Map(KYUUBI_SESSION_ALIVE_PROBE -> "false")).asJava)
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
        req.setConfiguration((configs ++ Map(
          KyuubiConf.SESSION_NAME.key -> sessionName,
          KYUUBI_SESSION_HANDLE_KEY -> UUID.randomUUID().toString,
          KyuubiConf.ENGINE_SESSION_INITIALIZE_SQL.key -> "",
          KYUUBI_SESSION_ALIVE_PROBE -> "true")).asJava)
        val resp = aliveProbeClient.OpenSession(req)
        ThriftUtils.verifyTStatus(resp.getStatus)
        _aliveProbeSessionHandle = resp.getSessionHandle
        startEngineAliveProbe()
      }
    }

    SessionHandle(_remoteSessionHandle)
  }

  def closeSession(): Unit = {
    if (clientClosedByAliveProbe || clientClosed.get()) return
    try {
      if (_remoteSessionHandle != null) {
        withCleanupRequest("CloseSession") { deadline =>
          withLockAcquiredAsyncRequest(
            {
              val req = new TCloseSessionReq(_remoteSessionHandle)
              val resp = CloseSession(req)
              ThriftUtils.verifyTStatus(resp.getStatus)
              Option(engineAliveThreadPool).foreach { pool =>
                ThreadUtils.shutdown(pool, deadline.timeLeft)
                if (!pool.isTerminated) {
                  throw new TimeoutException("Timed out stopping the engine aliveness probe")
                }
              }
              if (_aliveProbeSessionHandle != null) {
                engineAliveProbeClient.foreach { client =>
                  Utils.tryLogNonFatalError {
                    val probeReq = new TCloseSessionReq(_aliveProbeSessionHandle)
                    val probeResp = client.CloseSession(probeReq)
                    ThriftUtils.verifyTStatus(probeResp.getStatus)
                  }
                }
              }
            },
            Some(deadline))
        }
      }
    } catch {
      case e: Exception =>
        throw KyuubiSQLException(
          s"Error while cleaning up the engine resources: ${e.getMessage}",
          e)
    } finally {
      closeClient()
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

  def cancelOperation(operationHandle: TOperationHandle): Unit =
    withCleanupRequest("CancelOperation") { deadline =>
      cancelOperation(operationHandle, deadline)
    }

  private[kyuubi] def cancelOperation(
      operationHandle: TOperationHandle,
      deadline: Deadline): Unit = {
    val req = new TCancelOperationReq(operationHandle)
    val resp = withLockAcquiredAsyncRequest(CancelOperation(req), Some(deadline))
    if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
      info(s"$req succeed on engine side")
    } else {
      warn(s"$req failed on engine side", KyuubiSQLException(resp.getStatus))
    }
  }

  def closeOperation(operationHandle: TOperationHandle): Unit =
    withCleanupRequest("CloseOperation") { deadline =>
      closeOperation(operationHandle, deadline)
    }

  private[kyuubi] def closeOperation(
      operationHandle: TOperationHandle,
      deadline: Deadline): Unit = {
    val req = new TCloseOperationReq(operationHandle)
    val resp = withLockAcquiredAsyncRequest(CloseOperation(req), Some(deadline))
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
      case e: Exception =>
        warn(s"$req failed on engine side", e)
        // catch exception in HadoopCredentialsManager.sendCredentialsIfNeeded
        throw e
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
      connectionTimeout: Int,
      maxMessageSize: Int): TProtocol = {
    val tConf = TConfiguration.custom().setMaxMessageSize(maxMessageSize).build()
    val tSocket = new TSocket(tConf, host, port, socketTimeout, connectionTimeout)
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
    val maxMessageSize = conf.get(KyuubiConf.FRONTEND_THRIFT_CLIENT_MAX_MESSAGE_SIZE)
    val aliveProbeEnabled = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED)
    val aliveProbeInterval = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL).toInt
    val aliveTimeout = conf.get(KyuubiConf.ENGINE_ALIVE_TIMEOUT)
    val cleanupTimeout = conf.get(ENGINE_CLEANUP_TIMEOUT)
    val useVirtualThreadsForAliveProbe = conf.get(ENGINE_ALIVE_PROBE_VIRTUAL_THREADS_ENABLED)
    val useVirtualThreadsForAsyncRequests = conf.get(ENGINE_RPC_CLIENT_VIRTUAL_THREADS_ENABLED)

    val tProtocol = createTProtocol(user, passwd, host, port, 0, loginTimeout, maxMessageSize)

    val aliveProbeProtocol =
      if (aliveProbeEnabled) {
        Some(createTProtocol(
          user,
          passwd,
          host,
          port,
          aliveProbeInterval,
          loginTimeout,
          maxMessageSize))
      } else {
        None
      }
    new KyuubiSyncThriftClient(
      (host, port),
      tProtocol,
      aliveProbeProtocol,
      aliveProbeInterval,
      aliveTimeout,
      cleanupTimeout,
      useVirtualThreadsForAliveProbe,
      useVirtualThreadsForAsyncRequests)
  }
}
