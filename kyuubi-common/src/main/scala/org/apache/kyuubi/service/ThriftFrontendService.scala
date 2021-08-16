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

package org.apache.kyuubi.service

import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.server.{ServerContext, TServer, TServerEventHandler, TThreadPoolServer}
import org.apache.thrift.transport.{TServerSocket, TTransport}

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException, Logging}
import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle}
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.util.{ExecutorPoolCaptureOom, KyuubiHadoopUtils, NamedThreadFactory}

class ThriftFrontendService private(name: String, be: BackendService, oomHook: Runnable)
  extends AbstractFrontendService(name, be) with TCLIService.Iface with Runnable with Logging {

  import ThriftFrontendService._
  import KyuubiConf._

  def this(be: BackendService, oomHook: Runnable) = {
    this(classOf[ThriftFrontendService].getSimpleName, be, oomHook)
  }

  private var server: Option[TServer] = None
  private var serverThread: Thread = _
  protected var serverAddr: InetAddress = _
  protected var portNum: Int = _
  @volatile protected var isStarted = false

  private var authFactory: KyuubiAuthenticationFactory = _
  private var hadoopConf: Configuration = _

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    try {
      hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
      val serverHost = conf.get(FRONTEND_BIND_HOST)
      serverAddr = serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)
      portNum = conf.get(FRONTEND_BIND_PORT)
      val minThreads = conf.get(FRONTEND_MIN_WORKER_THREADS)
      val maxThreads = conf.get(FRONTEND_MAX_WORKER_THREADS)
      val keepAliveTime = conf.get(FRONTEND_WORKER_KEEPALIVE_TIME)
      val executor = ExecutorPoolCaptureOom(
        name + "Handler-Pool",
        minThreads, maxThreads,
        keepAliveTime,
        oomHook)
      authFactory = new KyuubiAuthenticationFactory(conf)
      val transFactory = authFactory.getTTransportFactory
      val tProcFactory = authFactory.getTProcessorFactory(this)
      val serverSocket = new ServerSocket(portNum, -1, serverAddr)
      portNum = serverSocket.getLocalPort
      val tServerSocket = new TServerSocket(serverSocket)

      val maxMessageSize = conf.get(FRONTEND_MAX_MESSAGE_SIZE)
      val requestTimeout = conf.get(FRONTEND_LOGIN_TIMEOUT).toInt
      val beBackoffSlotLength = conf.get(FRONTEND_LOGIN_BACKOFF_SLOT_LENGTH).toInt

      val args = new TThreadPoolServer.Args(tServerSocket)
        .processorFactory(tProcFactory)
        .transportFactory(transFactory)
        .protocolFactory(new TBinaryProtocol.Factory)
        .inputProtocolFactory(
          new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
        .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.MILLISECONDS)
        .beBackoffSlotLength(beBackoffSlotLength)
        .beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
        .executorService(executor)
      // TCP Server
      server = Some(new TThreadPoolServer(args))
      server.foreach(_.setServerEventHandler(new FeTServerEventHandler))
      info(s"Initializing $name on host ${serverAddr.getCanonicalHostName} at port $portNum with" +
        s" [$minThreads, $maxThreads] worker threads")
    } catch {
      case e: Throwable =>
        throw new KyuubiException(
          s"Failed to initialize frontend service on $serverAddr:$portNum.", e)
    }
    super.initialize(conf)
  }

  override def connectionUrl(server: Boolean = false): String = {
    getServiceState match {
      case s @ ServiceState.LATENT => throw new IllegalStateException(s"Illegal Service State: $s")
      case _ =>
        if (server || conf.get(ENGINE_CONNECTION_URL_USE_HOSTNAME)) {
          s"${serverAddr.getCanonicalHostName}:$portNum"
        } else {
          // engine use address if run on k8s with cluster mode
          s"${serverAddr.getHostAddress}:$portNum"
        }
    }
  }

  override def start(): Unit = synchronized {
    super.start()
    if(!isStarted) {
      serverThread = new NamedThreadFactory(getName, false).newThread(this)
      serverThread.start()
      isStarted = true
    }
  }

  override def run(): Unit = try {
    info(s"Starting and exposing JDBC connection at: jdbc:hive2://${connectionUrl(true)}/")
    server.foreach(_.serve())
  } catch {
    case _: InterruptedException => error(s"$getName is interrupted")
    case t: Throwable =>
      error(s"Error starting $getName", t)
      System.exit(-1)
  }

  override def stop(): Unit = synchronized {
    if (isStarted) {
      if (serverThread != null) {
        serverThread.interrupt()
        serverThread = null
      }
      server.foreach(_.stop())
      server = None
      info(this.name + " has stopped")
      isStarted = false
    }
    super.stop()
  }

  private def getProxyUser(
      sessionConf: java.util.Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    val proxyUser = sessionConf.get(KyuubiAuthenticationFactory.HS2_PROXY_USER)
    if (proxyUser == null) {
      realUser
    } else {
      KyuubiAuthenticationFactory.verifyProxyAccess(realUser, proxyUser, ipAddress, hadoopConf)
      proxyUser
    }
  }

  private def getUserName(req: TOpenSessionReq): String = {
    val realUser: String =
      ServiceUtils.getShortName(authFactory.getRemoteUser.getOrElse(req.getUsername))
    if (req.getConfiguration == null) {
      realUser
    } else {
      getProxyUser(req.getConfiguration, authFactory.getIpAddress.orNull, realUser)
    }
  }

  private def getMinVersion(versions: TProtocolVersion*): TProtocolVersion = {
    versions.minBy(_.getValue)
  }

  @throws[KyuubiSQLException]
  private def getSessionHandle(req: TOpenSessionReq, res: TOpenSessionResp): SessionHandle = {
    val protocol = getMinVersion(SERVER_VERSION, req.getClient_protocol)
    res.setServerProtocolVersion(protocol)
    val userName = getUserName(req)
    val ipAddress = authFactory.getIpAddress.orNull
    val configuration =
      Option(req.getConfiguration).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    val sessionHandle = be.openSession(
      protocol, userName, req.getPassword, ipAddress, configuration)
    sessionHandle
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)
      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      resp.setConfiguration(new java.util.HashMap[String, String]())
      resp.setStatus(OK_STATUS)
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        warn("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
    }
    resp
  }

  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    debug(req.toString)
    val handle = SessionHandle(req.getSessionHandle)
    info(s"Received request of closing $handle")
    val resp = new TCloseSessionResp
    try {
      be.closeSession(handle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error closing session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    } finally {
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(null))
    }
    info(s"Finished closing $handle")
    resp
  }

  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    debug(req.toString)
    val resp = new TGetInfoResp
    try {
      val infoValue = be.getInfo(SessionHandle(req.getSessionHandle), req.getInfoType)
      resp.setInfoValue(infoValue)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting type info: ", e)
        resp.setInfoValue(TGetInfoValue.lenValue(0))
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    debug(req.toString)
    val resp = new TExecuteStatementResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val statement = req.getStatement
      val runAsync = req.isRunAsync
      // val confOverlay = req.getConfOverlay
      val queryTimeout = req.getQueryTimeout
      val operationHandle = be.executeStatement(sessionHandle, statement, runAsync, queryTimeout)
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error executing statement: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    debug(req.toString)
    val resp = new TGetTypeInfoResp
    try {
      val operationHandle = be.getTypeInfo(SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting type info: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    debug(req.toString)
    val resp = new TGetCatalogsResp
    try {
      val opHandle = be.getCatalogs(SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting catalogs: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    debug(req.toString)
    val resp = new TGetSchemasResp
    try {
      val opHandle = be.getSchemas(
        SessionHandle(req.getSessionHandle), req.getCatalogName, req.getSchemaName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting schemas: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    debug(req.toString)
    val resp = new TGetTablesResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val table = req.getTableName
      val tableTypes = req.getTableTypes
      val opHandle = be.getTables(sessionHandle, catalog, schema, table, tableTypes)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting tables: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    debug(req.toString)
    val resp = new TGetTableTypesResp
    try {
      val opHandle = be.getTableTypes(SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting table types: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    debug(req.toString)
    val resp = new TGetColumnsResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val table = req.getTableName
      val col = req.getColumnName
      val opHandle = be.getColumns(sessionHandle, catalog, schema, table, col)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting columns: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    debug(req.toString)
    val resp = new TGetFunctionsResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val func = req.getFunctionName
      val opHandle = be.getFunctions(sessionHandle, catalog, schema, func)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting functions: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetPrimaryKeys(req: TGetPrimaryKeysReq): TGetPrimaryKeysResp = {
    debug(req.toString)
    val resp = new TGetPrimaryKeysResp
    val errStatus = KyuubiSQLException.featureNotSupported().toTStatus
    resp.setStatus(errStatus)
    resp
  }

  override def GetCrossReference(req: TGetCrossReferenceReq): TGetCrossReferenceResp = {
    debug(req.toString)
    val resp = new TGetCrossReferenceResp
    val errStatus = KyuubiSQLException.featureNotSupported().toTStatus
    resp.setStatus(errStatus)
    resp
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    debug(req.toString)
    val resp = new TGetOperationStatusResp
    try {
      val operationHandle = OperationHandle(req.getOperationHandle)
      val operationStatus = be.getOperationStatus(operationHandle)
      resp.setOperationState(operationStatus.state)
      resp.setOperationStarted(operationStatus.start)
      resp.setOperationCompleted(operationStatus.completed)
      resp.setHasResultSet(operationStatus.hasResultSet)
      operationStatus.exception.foreach { e =>
        resp.setSqlState(e.getSQLState)
        resp.setErrorCode(e.getErrorCode)
        resp.setErrorMessage(e.getMessage)
      }
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting operation status: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    debug(req.toString)
    val resp = new TCancelOperationResp
    try {
      be.cancelOperation(OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error cancelling operation: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    debug(req.toString)
    val resp = new TCloseOperationResp
    try {
      be.closeOperation(OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error closing operation: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    debug(req.toString)
    val resp = new TGetResultSetMetadataResp
    try {
      val schema = be.getResultSetMetadata(OperationHandle(req.getOperationHandle))
      resp.setSchema(schema)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting result set metadata: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    debug(req.toString)
    val resp = new TFetchResultsResp
    try {
      val operationHandle = OperationHandle(req.getOperationHandle)
      val orientation = FetchOrientation.getFetchOrientation(req.getOrientation)
      // 1 means fetching log
      val fetchLog = req.getFetchType == 1
      val maxRows = req.getMaxRows.toInt
      val rowSet = be.fetchResults(operationHandle, orientation, maxRows, fetchLog)
      resp.setResults(rowSet)
      resp.setHasMoreRows(false)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error fetching results: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  private def notSupportTokenErrorStatus = {
    val errorStatus = new TStatus(TStatusCode.ERROR_STATUS)
    errorStatus.setErrorMessage("Delegation token is not supported")
    errorStatus
  }

  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    debug(req.toString)
    val resp = new TGetDelegationTokenResp()
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }

  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    debug(req.toString)
    val resp = new TCancelDelegationTokenResp
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)
    val resp = new TRenewDelegationTokenResp
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }

  class FeTServerEventHandler extends TServerEventHandler {
    implicit def toFeServiceServerContext(context: ServerContext): FeServiceServerContext = {
      context.asInstanceOf[FeServiceServerContext]
    }

    override def deleteContext(context: ServerContext, in: TProtocol, out: TProtocol): Unit = {
      val handle = context.getSessionHandle
      if (handle != null) {
        info(s"Session [$handle] disconnected without closing properly, close it now")
        try {
          be.closeSession(handle)
        } catch {
          case e: KyuubiSQLException =>
            warn("Failed closing session", e)
        }
      }
    }

    override def processContext(context: ServerContext, in: TTransport, out: TTransport): Unit = {
      CURRENT_SERVER_CONTEXT.set(context)
    }

    override def preServe(): Unit = {}

    override def createContext(in: TProtocol, out: TProtocol): ServerContext = {
      new FeServiceServerContext()
    }
  }
}

object ThriftFrontendService {
  final val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)

  final val CURRENT_SERVER_CONTEXT = new ThreadLocal[FeServiceServerContext]()

  final val SERVER_VERSION = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10

  class FeServiceServerContext extends ServerContext {
    private var sessionHandle: SessionHandle = _

    def setSessionHandle(sessionHandle: SessionHandle): Unit = {
      this.sessionHandle = sessionHandle
    }

    def getSessionHandle: SessionHandle = sessionHandle
  }
}
