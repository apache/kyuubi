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
package org.apache.kyuubi.engine.spark.connect.grpc

import java.util

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class AbstractGrpcSession extends Session with Logging {

  final private val opHandleSet = new util.HashSet[OperationHandle]

  override def protocol: TProtocolVersion = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def handle: SessionHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def name: Option[String] = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def conf: Map[String, String] = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def user: String = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def password: String = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def ipAddress: String = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def createTime: Long = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def lastAccessTime: Long = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def lastIdleTime: Long = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getNoOperationTime: Long = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def sessionIdleTimeoutThreshold: Long = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def sessionManager: SessionManager = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def open(): Unit = {
    OperationLog.createOperationLogRootDirectory(this)
  }

  override def close(): Unit = {
    opHandleSet.forEach { opHandle =>
      try {
        sessionManager.operationManager.closeOperation(opHandle)
      } catch {
        case e: Exception =>
          warn(s"Error closing operation $opHandle during closing $handle for", e)
      }
    }
  }

  protected def runOperation(operation: Operation): OperationHandle = {
    try {
      val opHandle = operation.getHandle
      opHandleSet.add(opHandle)
      operation.run()
      opHandle
    } catch {
      case e: KyuubiSQLException =>
        opHandleSet.remove(operation.getHandle)
        sessionManager.operationManager.closeOperation(operation.getHandle)
        throw e
    }
  }
  override def getInfo(infoType: TGetInfoType): TGetInfoValue = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getTableTypes: OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getTypeInfo: OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCatalogs: OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getSchemas(catalogName: String, schemaName: String): OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getColumns(
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getFunctions(
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getPrimaryKeys(
      catalogName: String,
      schemaName: String,
      tableName: String): OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCrossReference(
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): OperationHandle = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getQueryId(operationHandle: OperationHandle): String = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def cancelOperation(operationHandle: OperationHandle): Unit = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def closeOperation(operationHandle: OperationHandle): Unit = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getResultSetMetadata(operationHandle: OperationHandle): TGetResultSetMetadataResp = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TFetchResultsResp = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def closeExpiredOperations(): Unit = {
    throw KyuubiSQLException.featureNotSupported()
  }
}
