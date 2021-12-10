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

import java.util

import org.apache.hive.service.rpc.thrift.{TGetOperationStatusResp, TOperationHandle, TProtocolVersion, TRowSet, TTableSchema}

import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.SessionHandle

class KyuubiThriftClientCallTimeStatics(client: KyuubiThriftClient) extends KyuubiThriftClient {

  private var timeCost: Long = 0L

  /**
   * Get the time cost millis on each call of the function in the [[KyuubiThriftClient]]
   */
  def getTimeCostMillis: Long = {
    timeCost
  }

  @throws[Exception]
  private def time[T](f: => T): T = {
    val startTime = System.currentTimeMillis()
    try {
      f
    } finally {
      timeCost += System.currentTimeMillis() - startTime
    }
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      configs: Map[String, String]): SessionHandle = {
    time(client.openSession(protocol, user, password, configs))
  }

  override def closeSession(): Unit = {
    time(client.closeSession())
  }

  override def executeStatement(
      statement: String,
      shouldRunAsync: Boolean,
      queryTimeout: Long): TOperationHandle = {
    time(client.executeStatement(statement, shouldRunAsync, queryTimeout))
  }

  override def getTypeInfo: TOperationHandle = {
    time(client.getTypeInfo)
  }

  override def getCatalogs: TOperationHandle = {
    time(client.getCatalogs)
  }

  override def getSchemas(catalogName: String, schemaName: String): TOperationHandle = {
    time(client.getSchemas(catalogName, schemaName))
  }

  override def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): TOperationHandle = {
    time(client.getTables(catalogName, schemaName, tableName, tableTypes))
  }

  override def getTableTypes: TOperationHandle = {
    time(client.getTableTypes)
  }

  override def getColumns(
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): TOperationHandle = {
    time(client.getColumns(catalogName, schemaName, tableName, columnName))
  }

  override def getFunctions(
      catalogName: String,
      schemaName: String,
      functionName: String): TOperationHandle = {
    time(client.getFunctions(catalogName, schemaName, functionName))
  }

  override def getOperationStatus(operationHandle: TOperationHandle): TGetOperationStatusResp = {
    time(client.getOperationStatus(operationHandle))
  }

  override def cancelOperation(operationHandle: TOperationHandle): Unit = {
    time(client.cancelOperation(operationHandle))
  }

  override def closeOperation(operationHandle: TOperationHandle): Unit = {
    time(client.cancelOperation(operationHandle))
  }

  override def getResultSetMetadata(operationHandle: TOperationHandle): TTableSchema = {
    time(client.getResultSetMetadata(operationHandle))
  }

  override def fetchResults(
      operationHandle: TOperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet = {
    time(client.fetchResults(operationHandle, orientation, maxRows, fetchLog))
  }

  override def sendCredentials(encodedCredentials: String): Unit = {
    time(client.sendCredentials(encodedCredentials))
  }
}
