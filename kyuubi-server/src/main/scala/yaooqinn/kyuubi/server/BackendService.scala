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

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.kyuubi.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.author.AuthzHelper
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType, GetInfoValue}
import yaooqinn.kyuubi.operation.{OperationHandle, OperationStatus}
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.service.CompositeService
import yaooqinn.kyuubi.session.{SessionHandle, SessionManager}

/**
 * [[BackendService]] holds an instance of [[SessionManager]] which manages
 * `KyuubiSession` for execution
 */
private[server] class BackendService private(name: String, server: KyuubiServer)
  extends CompositeService(name) with Logging {

  private[this] var sessionManager: SessionManager = _
  def getSessionManager: SessionManager = sessionManager

  def this(server: KyuubiServer) = this(classOf[BackendService].getSimpleName, server)

  override def init(conf: SparkConf): Unit = synchronized {
    this.conf = conf
    AuthzHelper.init(conf)
    sessionManager = new SessionManager(server)
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
      statement: String): OperationHandle = {
    sessionManager.getSession(sessionHandle).executeStatement(statement)
  }

  def executeStatementAsync(
      sessionHandle: SessionHandle,
      statement: String): OperationHandle = {
    sessionManager.getSession(sessionHandle).executeStatementAsync(statement)
  }

  def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getTypeInfo
  }

  def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getCatalogs
  }

  def getSchemas(
      sessionHandle: SessionHandle, catalogName: String, schemaName: String): OperationHandle = {
    sessionManager.getSession(sessionHandle).getSchemas(catalogName, schemaName)
  }

  def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: Seq[String]): OperationHandle = {
    sessionManager.getSession(sessionHandle)
      .getTables(catalogName, schemaName, tableName, tableTypes)
  }

  def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getTableTypes
  }

  def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String, tableName: String, columnName: String): OperationHandle = {
    sessionManager.getSession(sessionHandle)
      .getColumns(catalogName, schemaName, tableName, columnName)
  }

  def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    sessionManager.getSession(sessionHandle).getFunctions(catalogName, schemaName, functionName)
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

  def getResultSetMetadata(opHandle: OperationHandle): StructType = {
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
