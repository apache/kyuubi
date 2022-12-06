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
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.{ServerContext, TServerEventHandler}
import org.apache.thrift.transport.TTransport

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.Utils.stringifyException
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle}
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.util.{KyuubiHadoopUtils, NamedThreadFactory}

/**
 * Apache Thrift based hive-service-rpc base class
 *   1. http
 *   2. binary
 */
abstract class TFrontendService(name: String)
  extends AbstractFrontendService(name) with TCLIService.Iface with Runnable with Logging {
  import TFrontendService._
  private val started = new AtomicBoolean(false)
  private lazy val _hadoopConf: Configuration = KyuubiHadoopUtils.newHadoopConf(conf)
  private lazy val serverThread = new NamedThreadFactory(getName, false).newThread(this)

  protected def serverHost: Option[String]
  protected def portNum: Int
  protected lazy val serverAddr: InetAddress =
    serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)
  protected lazy val serverSocket = new ServerSocket(portNum, -1, serverAddr)
  protected lazy val actualPort: Int = serverSocket.getLocalPort
  protected lazy val authFactory: KyuubiAuthenticationFactory =
    new KyuubiAuthenticationFactory(conf, isServer())

  protected def hadoopConf: Configuration = _hadoopConf

  /**
   * Start the service itself(FE) and its composited (Discovery service, DS) in the order of:
   *   Start FE ->
   *     if (success) -> Continue starting DS
   *       if (success) -> finish
   *       else -> Stop DS -> Raise Error -> Stop FE -> Raise Error
   *     else
   *       Raise Error -> Stop FE -> Raise Error
   *    This makes sure that the FE has started and ready to serve before exposing through DS.
   */
  override def start(): Unit = synchronized {
    try {
      if (started.compareAndSet(false, true)) {
        serverThread.start()
      }
      super.start()
    } catch {
      case e: Throwable =>
        stopInternal()
        throw e
    }
  }

  protected def stopServer(): Unit

  /**
   * Inner stop progress that will not stop all services composited with this.
   */
  private def stopInternal(): Unit = {
    if (started.compareAndSet(true, false)) {
      serverThread.interrupt()
      stopServer()
      info(getName + " has stopped")
    }
  }

  /**
   * Stop the service itself(FE) and its composited (Discovery service, DS) in the order of:
   *   Stop DS -> Stop FE
   * This makes sure of
   *   1. The service stop serving before terminating during stopping
   *   2. For engines with group share level, the DS stopping is invoked by a pool in FE,
   *   so we need to stop DS first in case of interrupting.
   */
  override def stop(): Unit = synchronized {
    super.stop()
    stopInternal()
  }

  override def connectionUrl: String = {
    checkInitialized()
    val host = serverHost match {
      case Some(h) => h // respect user's setting ahead
      case None if conf.get(FRONTEND_CONNECTION_URL_USE_HOSTNAME) =>
        serverAddr.getCanonicalHostName
      case None =>
        serverAddr.getHostAddress
    }

    host + ":" + actualPort
  }

  protected def getProxyUser(
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

  /**
   * Get the real user and the session user.
   * The real user is the user used for session authentication.
   * The session user is the proxy user if proxy user is provided, otherwise is the real user.
   */
  protected def getRealUserAndSessionUser(req: TOpenSessionReq): (String, String) = {
    val realUser: String =
      ServiceUtils.getShortName(authFactory.getRemoteUser.getOrElse(req.getUsername))
    val sessionUser =
      if (req.getConfiguration == null) {
        realUser
      } else {
        getProxyUser(req.getConfiguration, authFactory.getIpAddress.orNull, realUser)
      }
    realUser -> sessionUser
  }

  protected def getIpAddress: String = {
    authFactory.getIpAddress.orNull
  }

  private def getMinVersion(versions: TProtocolVersion*): TProtocolVersion = {
    versions.minBy(_.getValue)
  }

  @throws[KyuubiSQLException]
  protected def getSessionHandle(req: TOpenSessionReq, res: TOpenSessionResp): SessionHandle = {
    val protocol = getMinVersion(SERVER_VERSION, req.getClient_protocol)
    res.setServerProtocolVersion(protocol)
    val (realUser, sessionUser) = getRealUserAndSessionUser(req)
    val ipAddress = getIpAddress
    val configuration =
      Map(KYUUBI_CLIENT_IP_KEY -> ipAddress, KYUUBI_SERVER_IP_KEY -> serverAddr.getHostAddress) ++
        Option(req.getConfiguration).map(_.asScala.toMap).getOrElse(Map.empty[String, String]) ++
        Map(
          KYUUBI_SESSION_CONNECTION_URL_KEY -> connectionUrl,
          KYUUBI_SESSION_REAL_USER_KEY -> realUser)
    val sessionHandle = be.openSession(
      protocol,
      sessionUser,
      req.getPassword,
      ipAddress,
      configuration)
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
        error("Error opening session: ", e)
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
        error("Error closing session: ", e)
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
        error("Error getting type info: ", e)
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
      val confOverlay = Option(req.getConfOverlay).getOrElse(Map.empty.asJava)
      val queryTimeout = req.getQueryTimeout
      val operationHandle = be.executeStatement(
        sessionHandle,
        statement,
        confOverlay.asScala.toMap,
        runAsync,
        queryTimeout)
      val tOperationHandle = operationHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.EXECUTE_STATEMENT)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error executing statement: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    debug(req.toString)
    val resp = new TGetTypeInfoResp
    try {
      val operationHandle = be.getTypeInfo(SessionHandle(req.getSessionHandle))
      val tOperationHandle = operationHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_TYPE_INFO)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting type info: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    debug(req.toString)
    val resp = new TGetCatalogsResp
    try {
      val opHandle = be.getCatalogs(SessionHandle(req.getSessionHandle))
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_CATALOGS)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting catalogs: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    debug(req.toString)
    val resp = new TGetSchemasResp
    try {
      val opHandle = be.getSchemas(
        SessionHandle(req.getSessionHandle),
        req.getCatalogName,
        req.getSchemaName)
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_SCHEMAS)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting schemas: ", e)
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
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_TABLES)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting tables: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    debug(req.toString)
    val resp = new TGetTableTypesResp
    try {
      val opHandle = be.getTableTypes(SessionHandle(req.getSessionHandle))
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_TABLE_TYPES)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting table types: ", e)
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
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_COLUMNS)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting columns: ", e)
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
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_FUNCTIONS)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting functions: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetPrimaryKeys(req: TGetPrimaryKeysReq): TGetPrimaryKeysResp = {
    debug(req.toString)
    val resp = new TGetPrimaryKeysResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val catalog = req.getCatalogName
      val schema = req.getSchemaName
      val table = req.getTableName
      val opHandle = be.getPrimaryKeys(sessionHandle, catalog, schema, table)
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_FUNCTIONS)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting primary keys: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetCrossReference(req: TGetCrossReferenceReq): TGetCrossReferenceResp = {
    debug(req.toString)
    val resp = new TGetCrossReferenceResp
    try {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val primaryCatalog = req.getParentCatalogName
      val primarySchema = req.getParentSchemaName
      val primaryTable = req.getParentTableName
      val foreignCatalog = req.getForeignCatalogName
      val foreignSchema = req.getForeignSchemaName
      val foreignTable = req.getForeignTableName
      val opHandle = be.getCrossReference(
        sessionHandle,
        primaryCatalog,
        primarySchema,
        primaryTable,
        foreignCatalog,
        foreignSchema,
        foreignTable)
      val tOperationHandle = opHandle.toTOperationHandle
      tOperationHandle.setOperationType(TOperationType.GET_FUNCTIONS)
      resp.setOperationHandle(tOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting primary keys: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
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
        resp.setErrorMessage(stringifyException(e))
      }
      operationStatus.operationProgressUpdate.foreach { p =>
        resp.setProgressUpdateResponse(p)
      }
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        error("Error getting operation status: ", e)
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
        error("Error cancelling operation: ", e)
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
        error("Error closing operation: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    debug(req.toString)
    try {
      be.getResultSetMetadata(OperationHandle(req.getOperationHandle))
    } catch {
      case e: Exception =>
        error("Error getting result set metadata: ", e)
        val resp = new TGetResultSetMetadataResp
        resp.setStatus(KyuubiSQLException.toTStatus(e))
        resp
    }
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
        error("Error fetching results: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  protected def notSupportTokenErrorStatus = {
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
    resp.setStatus(OK_STATUS)
    resp
  }

  override def GetQueryId(req: TGetQueryIdReq): TGetQueryIdResp = {
    debug(req.toString)
    val resp = new TGetQueryIdResp
    val queryId = be.getQueryId(OperationHandle(req.getOperationHandle))
    resp.setQueryId(queryId)
    resp
  }

  override def SetClientInfo(req: TSetClientInfoReq): TSetClientInfoResp = {
    debug(req.toString)
    val resp = new TSetClientInfoResp
    if (req.isSetConfiguration) {
      val sessionHandle = SessionHandle(req.getSessionHandle)
      val stringBuilder = new StringBuilder("Client information for ")
        .append(sessionHandle)
        .append(": ")
      val entries = req.getConfiguration.entrySet.asScala.toSeq
      entries.headOption.foreach(e => {
        stringBuilder.append(e.getKey).append(" = ").append(e.getValue)
      })
      entries.tail.foreach { e =>
        stringBuilder.append(", ")
        stringBuilder.append(e.getKey).append(" = ").append(e.getValue)
      }
      info(stringBuilder.toString())
    }
    resp.setStatus(OK_STATUS)
    resp
  }

  protected def isServer(): Boolean = false

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
            error("Failed closing session", e)
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

private[kyuubi] object TFrontendService {
  final val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)

  final val CURRENT_SERVER_CONTEXT = new ThreadLocal[FeServiceServerContext]()

  final val SERVER_VERSION = TProtocolVersion.values.max

  class FeServiceServerContext extends ServerContext {
    private var sessionHandle: SessionHandle = _

    def setSessionHandle(sessionHandle: SessionHandle): Unit = {
      this.sessionHandle = sessionHandle
    }

    def getSessionHandle: SessionHandle = sessionHandle
  }

  def okStatusWithHint(hint: Seq[String]): TStatus = {
    val ok = new TStatus(TStatusCode.SUCCESS_STATUS)
    ok.setInfoMessages(hint.asJava)
    ok
  }
}
