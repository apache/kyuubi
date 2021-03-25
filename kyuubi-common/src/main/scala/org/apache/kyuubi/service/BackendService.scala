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

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.operation.{OperationHandle, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.{SessionHandle, SessionManager}

/**
 * A [[BackendService]] in Kyuubi architecture is responsible for talking to the SQL engine
 *
 * 1. Open/Close [[org.apache.kyuubi.session.Session]] <br/>
 * 2. Operate [[org.apache.kyuubi.operation.Operation]] <br/>
 * 3. Manager [[org.apache.kyuubi.session.Session]]s via [[SessionManager]] <br/>
 * 4. Check [[OperationStatus]] <br/>
 * 5. Retrieve [[org.apache.kyuubi.operation.Operation]] results and metadata <br/>
 * 6. Cancel/Close [[org.apache.kyuubi.operation.Operation]] <br/>
 *
 */
trait BackendService {

  def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddr: String,
      configs: Map[String, String]): SessionHandle
  def closeSession(sessionHandle: SessionHandle): Unit

  def getInfo(sessionHandle: SessionHandle, infoType: TGetInfoType): TGetInfoValue

  def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      queryTimeout: Long): OperationHandle
  def executeStatementAsync(
      sessionHandle: SessionHandle,
      statement: String,
      queryTimeout: Long): OperationHandle

  def getTypeInfo(sessionHandle: SessionHandle): OperationHandle
  def getCatalogs(sessionHandle: SessionHandle): OperationHandle
  def getSchemas(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String): OperationHandle
  def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): OperationHandle
  def getTableTypes(sessionHandle: SessionHandle): OperationHandle
  def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle
  def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle

  def getOperationStatus(operationHandle: OperationHandle): OperationStatus
  def cancelOperation(operationHandle: OperationHandle): Unit
  def closeOperation(operationHandle: OperationHandle): Unit
  def getResultSetMetadata(operationHandle: OperationHandle): TTableSchema
  def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet

  def sessionManager: SessionManager
}
