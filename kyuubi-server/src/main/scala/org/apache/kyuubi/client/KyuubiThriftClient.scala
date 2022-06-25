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

import org.apache.hive.service.rpc.thrift.{TGetOperationStatusResp, TGetQueryIdResp, TOperationHandle, TProtocolVersion, TRowSet, TSessionHandle, TTableSchema}
import org.apache.thrift.protocol.TProtocol

import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.SessionHandle

/**
 * The interface for internal thrift client between Kyuubi server and Kyuubi engine.
 */
trait KyuubiThriftClient {

  val protocol: TProtocol

  def remoteSessionHandle: Option[TSessionHandle]

  def engineId: Option[String]

  /**
   * Return the engine SessionHandle for kyuubi session so that we can get the same session id.
   */
  def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      configs: Map[String, String],
      engineSessionHandle: Option[TSessionHandle] = None): SessionHandle

  def closeSession(): Unit

  def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      shouldRunAsync: Boolean,
      queryTimeout: Long): TOperationHandle

  def getTypeInfo: TOperationHandle

  def getCatalogs: TOperationHandle

  def getSchemas(catalogName: String, schemaName: String): TOperationHandle

  def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): TOperationHandle

  def getTableTypes: TOperationHandle

  def getColumns(
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): TOperationHandle

  def getFunctions(
      catalogName: String,
      schemaName: String,
      functionName: String): TOperationHandle

  def getPrimaryKeys(
      catalogName: String,
      schemaName: String,
      tableName: String): TOperationHandle

  def getCrossReference(
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): TOperationHandle

  def getQueryId(operationHandle: TOperationHandle): TGetQueryIdResp

  def getOperationStatus(operationHandle: TOperationHandle): TGetOperationStatusResp

  def cancelOperation(operationHandle: TOperationHandle): Unit

  def closeOperation(operationHandle: TOperationHandle): Unit

  def getResultSetMetadata(operationHandle: TOperationHandle): TTableSchema

  def fetchResults(
      operationHandle: TOperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet

  def sendCredentials(encodedCredentials: String): Unit
}
