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

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_CLIENT_IP_KEY
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

abstract class AbstractSession(
    val protocol: TProtocolVersion,
    val user: String,
    val password: String,
    val ipAddress: String,
    val conf: Map[String, String],
    val sessionManager: SessionManager) extends Session with Logging {
  override val handle: SessionHandle = SessionHandle()

  def clientIpAddress: String = conf.getOrElse(KYUUBI_CLIENT_IP_KEY, ipAddress)
  protected def logSessionInfo(msg: String): Unit = info(s"[$user:$clientIpAddress] $handle - $msg")

  final private val _createTime: Long = System.currentTimeMillis()
  override def createTime: Long = _createTime

  @volatile private var _lastAccessTime: Long = _createTime
  override def lastAccessTime: Long = _lastAccessTime

  @volatile private var _lastIdleTime: Long = _createTime
  override def lastIdleTime: Long = _lastIdleTime

  override def getNoOperationTime: Long = {
    if (lastIdleTime > 0) System.currentTimeMillis() - _lastIdleTime else 0
  }

  override val sessionIdleTimeoutThreshold: Long = {
    conf.get(SESSION_IDLE_TIMEOUT.key)
      .map(_.toLong)
      .getOrElse(
        sessionManager.getConf.get(SESSION_IDLE_TIMEOUT))
  }

  val normalizedConf: Map[String, String] = sessionManager.validateAndNormalizeConf(conf)

  override lazy val name: Option[String] = normalizedConf.get(SESSION_NAME.key)

  final private val opHandleSet = ConcurrentHashMap.newKeySet[OperationHandle]()

  private def acquire(userAccess: Boolean): Unit = synchronized {
    if (userAccess) {
      _lastAccessTime = System.currentTimeMillis
    }
    _lastIdleTime = 0
  }

  private def release(userAccess: Boolean): Unit = {
    if (userAccess) {
      _lastAccessTime = System.currentTimeMillis
    }
    if (opHandleSet.isEmpty) {
      _lastIdleTime = System.currentTimeMillis
    }
  }

  protected def withAcquireRelease[T](userAccess: Boolean = true)(f: => T): T = {
    acquire(userAccess)
    try f
    finally release(userAccess)
  }

  override def close(): Unit = withAcquireRelease() {
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

  override def getInfo(infoType: TGetInfoType): TGetInfoValue = withAcquireRelease() {
    infoType match {
      case TGetInfoType.CLI_SERVER_NAME | TGetInfoType.CLI_DBMS_NAME =>
        TGetInfoValue.stringValue("Apache Kyuubi")
      case TGetInfoType.CLI_DBMS_VER => TGetInfoValue.stringValue(org.apache.kyuubi.KYUUBI_VERSION)
      case TGetInfoType.CLI_ODBC_KEYWORDS => TGetInfoValue.stringValue("Unimplemented")
      case TGetInfoType.CLI_MAX_COLUMN_NAME_LEN |
          TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN |
          TGetInfoType.CLI_MAX_TABLE_NAME_LEN => TGetInfoValue.lenValue(128)
      case _ => throw KyuubiSQLException(s"Unrecognized GetInfoType value: $infoType")
    }
  }

  override def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = withAcquireRelease() {
    val operation = sessionManager.operationManager
      .newExecuteStatementOperation(this, statement, confOverlay, runAsync, queryTimeout)
    runOperation(operation)
  }

  override def getTableTypes: OperationHandle = withAcquireRelease() {
    val operation = sessionManager.operationManager.newGetTableTypesOperation(this)
    runOperation(operation)
  }

  override def getTypeInfo: OperationHandle = {
    val operation = sessionManager.operationManager.newGetTypeInfoOperation(this)
    runOperation(operation)
  }

  override def getCatalogs: OperationHandle = {
    val operation = sessionManager.operationManager.newGetCatalogsOperation(this)
    runOperation(operation)
  }

  override def getSchemas(catalogName: String, schemaName: String): OperationHandle = {
    val operation = sessionManager.operationManager
      .newGetSchemasOperation(this, catalogName, schemaName)
    runOperation(operation)
  }

  override def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): OperationHandle = {
    val operation = sessionManager.operationManager
      .newGetTablesOperation(this, catalogName, schemaName, tableName, tableTypes)
    runOperation(operation)
  }

  override def getColumns(
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    val operation = sessionManager.operationManager
      .newGetColumnsOperation(this, catalogName, schemaName, tableName, columnName)
    runOperation(operation)
  }

  override def getFunctions(
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    val operation = sessionManager.operationManager
      .newGetFunctionsOperation(this, catalogName, schemaName, functionName)
    runOperation(operation)
  }

  override def getPrimaryKeys(
      catalogName: String,
      schemaName: String,
      tableName: String): OperationHandle = {
    val operation = sessionManager.operationManager
      .newGetPrimaryKeysOperation(this, catalogName, schemaName, tableName)
    runOperation(operation)
  }

  override def getCrossReference(
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): OperationHandle = {
    val operation = sessionManager.operationManager
      .newGetCrossReferenceOperation(
        this,
        primaryCatalog,
        primarySchema,
        primaryTable,
        foreignCatalog,
        foreignSchema,
        foreignTable)
    runOperation(operation)
  }

  override def getQueryId(operationHandle: OperationHandle): String = {
    val operation = sessionManager.operationManager.getOperation(operationHandle)
    val queryId = sessionManager.operationManager.getQueryId(operation)
    queryId
  }

  override def cancelOperation(operationHandle: OperationHandle): Unit = withAcquireRelease() {
    sessionManager.operationManager.cancelOperation(operationHandle)
  }

  override def closeOperation(operationHandle: OperationHandle): Unit = withAcquireRelease() {
    opHandleSet.remove(operationHandle)
    sessionManager.operationManager.closeOperation(operationHandle)
  }

  override def getResultSetMetadata(
      operationHandle: OperationHandle): TGetResultSetMetadataResp = withAcquireRelease() {
    sessionManager.operationManager.getOperationResultSetSchema(operationHandle)
  }

  override def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TFetchResultsResp = {
    if (fetchLog) {
      sessionManager.operationManager.getOperationLogRowSet(operationHandle, orientation, maxRows)
    } else {
      sessionManager.operationManager.getOperationNextRowSet(operationHandle, orientation, maxRows)
    }
  }

  override def closeExpiredOperations(): Unit = {
    val operations = sessionManager.operationManager
      .removeExpiredOperations(opHandleSet.asScala.toSeq)
    operations.foreach { op =>
      // After the last expired Handle has been cleaned, the 'lastIdleTime' needs to be updated.
      withAcquireRelease(false) {
        opHandleSet.remove(op.getHandle)
        try {
          op.close()
        } catch {
          case e: Exception => warn(s"Error closing timed-out operation ${op.getHandle}", e)
        }
      }
    }
  }

  protected var operationalLogRootDir: Option[Path] = None

  override def open(): Unit = {
    operationalLogRootDir = Option(OperationLog.createOperationLogRootDirectory(this))
  }

  val isForAliveProbe: Boolean =
    conf.get(KyuubiReservedKeys.KYUUBI_SESSION_ALIVE_PROBE).exists(_.equalsIgnoreCase("true"))
}
