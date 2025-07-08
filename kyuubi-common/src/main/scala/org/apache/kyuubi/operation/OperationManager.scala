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

package org.apache.kyuubi.operation

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState._
import org.apache.kyuubi.operation.log.LogDivertAppender
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.ThreadUtils

/**
 * The [[OperationManager]] manages all the operations during their lifecycle.
 *
 * @param name Service Name
 */
abstract class OperationManager(name: String) extends AbstractService(name) {

  final private val handleToOperation = new java.util.HashMap[OperationHandle, Operation]()

  protected def skipOperationLog: Boolean = false

  /* Scheduler used for query timeout tasks */
  @volatile private var timeoutScheduler: ScheduledExecutorService = _

  def getOperationCount: Int = handleToOperation.size()

  def allOperations(): Iterable[Operation] = handleToOperation.values().asScala

  override def initialize(conf: KyuubiConf): Unit = {
    LogDivertAppender.initialize(skipOperationLog)
    super.initialize(conf)

    val poolSize = conf.get(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE)
    val keepAlive = conf.get(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME)
    timeoutScheduler = ThreadUtils.newDaemonScheduledThreadPool(
      poolSize,
      keepAlive,
      "operation-timeout")
  }

  override def stop(): Unit = synchronized {
    if (timeoutScheduler != null) {
      ThreadUtils.shutdown(timeoutScheduler)
      timeoutScheduler = null
    }
    super.stop()
  }

  /** Schedule a timeout task using the internal scheduler */
  def scheduleTimeout(action: Runnable, timeoutSeconds: Long): ScheduledFuture[_] = {
    timeoutScheduler.schedule(action, timeoutSeconds, TimeUnit.SECONDS)
  }

  def cancelTimeout(future: ScheduledFuture[_]): Unit = {
    if (future != null && !future.isCancelled) {
      future.cancel(false)
    }
  }

  def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation
  def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation
  def newGetCurrentCatalogOperation(session: Session): Operation
  def newSetCurrentDatabaseOperation(session: Session, database: String): Operation
  def newGetCurrentDatabaseOperation(session: Session): Operation
  def newGetTypeInfoOperation(session: Session): Operation
  def newGetCatalogsOperation(session: Session): Operation
  def newGetSchemasOperation(session: Session, catalog: String, schema: String): Operation
  def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): Operation
  def newGetTableTypesOperation(session: Session): Operation
  def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation
  def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation
  def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation
  def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation
  def getQueryId(operation: Operation): String

  final def addOperation(operation: Operation): Operation = synchronized {
    handleToOperation.put(operation.getHandle, operation)
    operation
  }

  @throws[KyuubiSQLException]
  final def getOperation(opHandle: OperationHandle): Operation = {
    val operation = synchronized { handleToOperation.get(opHandle) }
    if (operation == null) throw KyuubiSQLException(s"Invalid $opHandle")
    operation
  }

  @throws[KyuubiSQLException]
  final def removeOperation(opHandle: OperationHandle): Operation = synchronized {
    val operation = handleToOperation.remove(opHandle)
    if (operation == null) throw KyuubiSQLException(s"Invalid $opHandle")
    operation
  }

  @throws[KyuubiSQLException]
  final def cancelOperation(opHandle: OperationHandle): Unit = {
    val operation = getOperation(opHandle)
    operation.getStatus.state match {
      case CANCELED | CLOSED | FINISHED | ERROR | UNKNOWN =>
      case _ => operation.cancel()
    }
  }

  @throws[KyuubiSQLException]
  final def closeOperation(opHandle: OperationHandle): Unit = {
    val operation = removeOperation(opHandle)
    operation.close()
  }

  final def getOperationResultSetSchema(opHandle: OperationHandle): TGetResultSetMetadataResp = {
    getOperation(opHandle).getResultSetMetadata
  }

  final def getOperationNextRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TFetchResultsResp = {
    getOperation(opHandle).getNextRowSet(order, maxRows)
  }

  def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TFetchResultsResp = {
    val operationLog = getOperation(opHandle).getOperationLog
    val rowSet = operationLog.map(_.read(order, maxRows)).getOrElse {
      throw KyuubiSQLException(s"$opHandle failed to generate operation log")
    }
    val resp = new TFetchResultsResp(new TStatus(TStatusCode.SUCCESS_STATUS))
    resp.setResults(rowSet)
    resp.setHasMoreRows(false)
    resp
  }

  final def removeExpiredOperations(handles: Seq[OperationHandle]): Seq[Operation] = synchronized {
    handles.map(handleToOperation.get).filter { operation =>
      val isTimeout = operation != null && operation.isTimedOut
      if (isTimeout) {
        handleToOperation.remove(operation.getHandle)
        warn("Operation " + operation.getHandle + " is timed-out and will be closed")
        isTimeout
      } else {
        false
      }
    }
  }

  private val PATTERN_FOR_SET_CATALOG = "_SET_CATALOG"
  private val PATTERN_FOR_GET_CATALOG = "_GET_CATALOG"
  private val PATTERN_FOR_SET_SCHEMA = "(?i)use (.*)".r
  private val PATTERN_FOR_GET_SCHEMA = "select current_database()"

  final def processCatalogDatabase(
      session: Session,
      statement: String,
      confOverlay: Map[String, String]): Operation = {
    if (confOverlay.contains(KYUUBI_OPERATION_SET_CURRENT_CATALOG)
      && statement == PATTERN_FOR_SET_CATALOG) {
      newSetCurrentCatalogOperation(session, confOverlay(KYUUBI_OPERATION_SET_CURRENT_CATALOG))
    } else if (confOverlay.contains(KYUUBI_OPERATION_GET_CURRENT_CATALOG)
      && statement == PATTERN_FOR_GET_CATALOG) {
      newGetCurrentCatalogOperation(session)
    } else if (confOverlay.contains(KYUUBI_OPERATION_SET_CURRENT_DATABASE)
      && PATTERN_FOR_SET_SCHEMA.unapplySeq(statement).isDefined) {
      newSetCurrentDatabaseOperation(session, confOverlay(KYUUBI_OPERATION_SET_CURRENT_DATABASE))
    } else if (confOverlay.contains(KYUUBI_OPERATION_GET_CURRENT_DATABASE)
      && PATTERN_FOR_GET_SCHEMA == statement.toLowerCase) {
      newGetCurrentDatabaseOperation(session)
    } else {
      null
    }
  }
}
