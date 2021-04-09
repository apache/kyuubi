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

package org.apache.kyuubi.session

import org.apache.hive.service.rpc.thrift.{TGetInfoType, TGetInfoValue, TProtocolVersion, TRowSet, TTableSchema}

import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationHandle

trait Session {

  def protocol: TProtocolVersion
  def handle: SessionHandle

  def conf: Map[String, String]

  def user: String
  def password: String
  def ipAddress: String

  def createTime: Long
  def lastAccessTime: Long
  def lastIdleTime: Long
  def getNoOperationTime: Long

  def sessionManager: SessionManager

  def open(): Unit
  def close(): Unit


  def getInfo(infoType: TGetInfoType): TGetInfoValue

  def executeStatement(statement: String): OperationHandle
  def executeStatement(statement: String, queryTimeout: Long): OperationHandle
  def executeStatementAsync(statement: String): OperationHandle
  def executeStatementAsync(statement: String, queryTimeout: Long): OperationHandle

  def getTableTypes: OperationHandle
  def getTypeInfo: OperationHandle
  def getCatalogs: OperationHandle
  def getSchemas(catalogName: String, schemaName: String): OperationHandle
  def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): OperationHandle
  def getColumns(
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle
  def getFunctions(
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle

  def cancelOperation(operationHandle: OperationHandle): Unit
  def closeOperation(operationHandle: OperationHandle): Unit
  def getResultSetMetadata(operationHandle: OperationHandle): TTableSchema
  def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet

  def closeExpiredOperations: Unit
}
