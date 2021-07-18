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

import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}

import scala.concurrent.CancellationException

import org.apache.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion, TRowSet, TTableSchema}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.{OperationHandle, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.SessionHandle

/**
 * A Shorthand for implementing [[BackendService]]s
 */
abstract class AbstractBackendService(name: String)
  extends CompositeService(name) with BackendService {

  private lazy val timeout = conf.get(KyuubiConf.OPERATION_STATUS_POLLING_TIMEOUT)

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddr: String,
      configs: Map[String, String]): SessionHandle = {
    sessionManager.openSession(protocol, user, password, ipAddr, configs)
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    sessionManager.closeSession(sessionHandle)
  }

  override def getInfo(sessionHandle: SessionHandle, infoType: TGetInfoType): TGetInfoValue = {
    sessionManager.getSession(sessionHandle).getInfo(infoType)
  }

  override def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = {
    sessionManager.getSession(sessionHandle).executeStatement(statement, runAsync, queryTimeout)
  }

  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getTypeInfo
  }

  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getCatalogs
  }

  override def getSchemas(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String): OperationHandle = {
    sessionManager
      .getSession(sessionHandle)
      .getSchemas(catalogName, schemaName)
  }

  override def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): OperationHandle = {
    sessionManager
      .getSession(sessionHandle)
      .getTables(catalogName, schemaName, tableName, tableTypes)
  }

  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager
      .getSession(sessionHandle)
      .getTableTypes
  }

  override def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    sessionManager
      .getSession(sessionHandle)
      .getColumns(catalogName, schemaName, tableName, columnName)
  }

  override def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    sessionManager
      .getSession(sessionHandle)
      .getFunctions(catalogName, schemaName, functionName)
  }

  override def getOperationStatus(operationHandle: OperationHandle): OperationStatus = {
    val operation = sessionManager.operationManager.getOperation(operationHandle)
    if (operation.shouldRunAsync) {
      try {
        operation.getBackgroundHandle.get(timeout, TimeUnit.MILLISECONDS)
      } catch {
        case e: TimeoutException =>
          debug(s"$operationHandle: Long polling timed out, ${e.getMessage}")
        case e: CancellationException =>
          debug(s"$operationHandle: The background operation was cancelled, ${e.getMessage}")
        case e: ExecutionException =>
          debug(s"$operationHandle: The background operation was aborted, ${e.getMessage}")
        case _: InterruptedException =>
      }
    }
    operation.getStatus

  }

  override def cancelOperation(operationHandle: OperationHandle): Unit = {
    sessionManager
      .operationManager
      .getOperation(operationHandle)
      .getSession
      .cancelOperation(operationHandle)
  }

  override def closeOperation(operationHandle: OperationHandle): Unit = {
    sessionManager.operationManager
      .getOperation(operationHandle).getSession.closeOperation(operationHandle)
  }

  override def getResultSetMetadata(operationHandle: OperationHandle): TTableSchema = {
    sessionManager.operationManager
      .getOperation(operationHandle).getSession.getResultSetMetadata(operationHandle)
  }

  override def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet = {
    sessionManager.operationManager
      .getOperation(operationHandle)
      .getSession
      .fetchResults(operationHandle, orientation, maxRows, fetchLog)
  }

  override def initialize(conf: KyuubiConf): Unit = {
    addService(sessionManager)
    super.initialize(conf)
  }
}
