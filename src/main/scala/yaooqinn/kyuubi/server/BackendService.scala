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

package yaooqinn.kyuubi.server

import java.util.{List => JList, Map => JMap}

import org.apache.hive.service.cli._
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.SparkConf

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.operation.{OperationHandle, OperationStatus}
import yaooqinn.kyuubi.service.CompositeService
import yaooqinn.kyuubi.session.{SessionHandle, SessionManager}

/**
 * [[BackendService]] holds an instance of [[SessionManager]] which manages
 * `KyuubiSession` for execution
 */
private[server] class BackendService private(name: String)
  extends CompositeService(name) with Logging {

  private[this] var sessionManager: SessionManager = _
  def getSessionManager: SessionManager = sessionManager

  def this() = this(classOf[BackendService].getSimpleName)

  override def init(conf: SparkConf): Unit = synchronized {
    this.conf = conf
    sessionManager = new SessionManager()
    addService(sessionManager)
    super.init(conf)
  }

  override def start(): Unit = {
    super.start()
  }

  def openSession(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      configuration: Map[String, String]): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      protocol, username, password, ipAddress, configuration, withImpersonation = false)
    sessionHandle
  }

  def openSessionWithImpersonation(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      configuration: Map[String, String],
      delegationToken: String): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      protocol, username, password, ipAddress, configuration, withImpersonation = true)
    sessionHandle
  }

  def closeSession(sessionHandle: SessionHandle): Unit = {
    sessionManager.closeSession(sessionHandle)
  }

  def getInfo(sessionHandle: SessionHandle, infoType: GetInfoType): GetInfoValue = {
    sessionManager.getSession(sessionHandle).getInfo(infoType)
  }

  def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: JMap[String, String]): OperationHandle = {
    sessionManager.getSession(sessionHandle).executeStatement(statement)
  }

  def executeStatementAsync(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: JMap[String, String]): OperationHandle = {
    sessionManager.getSession(sessionHandle).executeStatementAsync(statement)
  }

  def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  def getSchemas(
      sessionHandle: SessionHandle, catalogName: String, schemaName: String): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: JList[String]): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String, tableName: String, columnName: String): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String, functionName: String): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  def getOperationStatus(opHandle: OperationHandle): OperationStatus = {
    sessionManager.getOperationMgr.getOperation(opHandle).getStatus
  }

  def cancelOperation(opHandle: OperationHandle): Unit = {
    sessionManager.getOperationMgr.getOperation(opHandle).getSession.cancelOperation(opHandle)
  }

  def closeOperation(opHandle: OperationHandle): Unit = {
    sessionManager.getOperationMgr.getOperation(opHandle).getSession.closeOperation(opHandle)
  }

  def getResultSetMetadata(opHandle: OperationHandle): TableSchema = {
    sessionManager.getOperationMgr.getOperation(opHandle).getSession.getResultSetMetadata(opHandle)
  }

  def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    sessionManager.getOperationMgr.getOperation(opHandle)
      .getSession.fetchResults(opHandle, orientation, maxRows, fetchType)
  }

  def getDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: KyuubiAuthFactory,
      owner: String,
      renewer: String): String = {
    info(sessionHandle + ": getDelegationToken()")
    sessionManager.getSession(sessionHandle).getDelegationToken(authFactory, owner, renewer)
  }

  def cancelDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: KyuubiAuthFactory,
      tokenStr: String): Unit = {
    info(sessionHandle + ": cancelDelegationToken()")
    sessionManager.getSession(sessionHandle).cancelDelegationToken(authFactory, tokenStr)
  }

  def renewDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: KyuubiAuthFactory,
      tokenStr: String): Unit = {
    info(sessionHandle + ": renewDelegationToken()")
    sessionManager.getSession(sessionHandle).renewDelegationToken(authFactory, tokenStr)
  }
}

object BackendService {
  final val SERVER_VERSION = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
}
