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

package yaooqinn.kyuubi.operation

import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.log4j.Logger
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.KyuubiConf._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.metrics.MetricsSystem
import yaooqinn.kyuubi.operation.metadata._
import yaooqinn.kyuubi.operation.statement.{ExecuteStatementInClientMode, ExecuteStatementOperation}
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.service.AbstractService
import yaooqinn.kyuubi.session.KyuubiSession

private[kyuubi] class OperationManager private(name: String)
  extends AbstractService(name) with Logging {

  def this() = this(classOf[OperationManager].getSimpleName)

  private lazy val logSchema: StructType = new StructType().add("operation_log", "string")
  private val handleToOperation = new ConcurrentHashMap[OperationHandle, KyuubiOperation]
  private val userToOperationLog = new ConcurrentHashMap[String, OperationLog]()

  override def init(conf: SparkConf): Unit = synchronized {
    if (conf.get(LOGGING_OPERATION_ENABLED.key).toBoolean) {
      initOperationLogCapture()
    } else {
      debug("Operation level logging is turned off")
    }
    super.init(conf)
  }

  private def initOperationLogCapture(): Unit = {
    // Register another Appender (with the same layout) that talks to us.
    val ap = new LogDivertAppender(this)
    Logger.getRootLogger.addAppender(ap)
  }

  private def getOperationLogByThread: OperationLog = OperationLog.getCurrentOperationLog

  private def getOperationLogByName: OperationLog = {
    if (!userToOperationLog.isEmpty) {
      userToOperationLog.get(KyuubiSparkUtil.getCurrentUserName)
    } else {
      null
    }
  }

  def getOperationLog: OperationLog = {
    Option(getOperationLogByThread).getOrElse(getOperationLogByName)
  }

  def setOperationLog(user: String, log: OperationLog): Unit = {
    OperationLog.setCurrentOperationLog(log)
    userToOperationLog.put(Option(user).getOrElse(KyuubiSparkUtil.getCurrentUserName), log)
  }

  def unregisterOperationLog(user: String): Unit = {
    OperationLog.removeCurrentOperationLog()
    userToOperationLog.remove(user)
  }

  def newExecuteStatementOperation(
      parentSession: KyuubiSession,
      statement: String,
      runAsync: Boolean = true): ExecuteStatementOperation = synchronized {
    val operation = new ExecuteStatementInClientMode(parentSession, statement, runAsync)
    addOperation(operation)
    operation
  }

  def newGetCatalogsOperation(session: KyuubiSession): GetCatalogsOperation = {
    val operation = new GetCatalogsOperation(session)
    addOperation(operation)
    operation
  }

  def newGetTablesOperation(
      session: KyuubiSession,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: Seq[String]): GetTablesOperation = {
    val operation =
      new GetTablesOperation(session, catalogName, schemaName, tableName, tableTypes)
    addOperation(operation)
    operation
  }

  def newGetFunctionsOperation(
      session: KyuubiSession,
      catalogName: String,
      schemaName: String,
      functionName: String): GetFunctionsOperation = {
    val operation =
      new GetFunctionsOperation(session, catalogName, schemaName, functionName)
    addOperation(operation)
    operation
  }

  def newGetColumnsOperation(
      session: KyuubiSession,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): GetColumnsOperation = {
    val operation =
      new GetColumnsOperation(session, catalogName, schemaName, tableName, columnName)
    addOperation(operation)
    operation
  }

  def newGetSchemasOperation(
      session: KyuubiSession,
      catalogName: String,
      schemaName: String): GetSchemasOperation = {
    val operation =
      new GetSchemasOperation(session, catalogName, schemaName)
    addOperation(operation)
    operation
  }

  def newGetTableTypesOperation(session: KyuubiSession): GetTableTypesOperation = {
    val operation = new GetTableTypesOperation(session)
    addOperation(operation)
    operation
  }

  def newGetTypeInfoOperation(session: KyuubiSession): GetTypeInfoOperation = {
    val operation = new GetTypeInfoOperation(session)
    addOperation(operation)
    operation
  }

  def getOperation(operationHandle: OperationHandle): KyuubiOperation = {
    val operation = getOperationInternal(operationHandle)
    if (operation == null) {
      throw new KyuubiSQLException("Invalid OperationHandle " + operationHandle)
    }
    operation
  }

  private def getOperationInternal(operationHandle: OperationHandle) =
    handleToOperation.get(operationHandle)

  private def addOperation(operation: KyuubiOperation): Unit = {
    handleToOperation.put(operation.getHandle, operation)
  }

  private def removeOperation(opHandle: OperationHandle) =
    handleToOperation.remove(opHandle)

  private def removeTimedOutOperation(
      operationHandle: OperationHandle): Option[KyuubiOperation] = synchronized {
    Option(handleToOperation.get(operationHandle))
      .filter(_.isTimedOut)
      .map { _ =>
        MetricsSystem.get.foreach(_.OPEN_OPERATIONS.dec)
        handleToOperation.remove(operationHandle)
      }
  }

  def cancelOperation(opHandle: OperationHandle): Unit = {
    val operation = getOperation(opHandle)
    val opState = operation.getStatus.getState
    if ((opState eq CANCELED)
      || (opState eq CLOSED)
      || (opState eq FINISHED)
      || (opState eq ERROR)
      || (opState eq UNKNOWN)) {
      // Cancel should be a no-op in either cases
      debug(opHandle + ": Operation is already aborted in state - " + opState)
    }
    else {
      debug(opHandle + ": Attempting to cancel from state - " + opState)
      operation.cancel()
    }
  }

  @throws[KyuubiSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    val operation = removeOperation(opHandle)
    if (operation == null) throw new KyuubiSQLException("Operation does not exist!")
    MetricsSystem.get.foreach(_.OPEN_OPERATIONS.dec())
    operation.close()
  }

  @throws[KyuubiSQLException]
  def getOperationNextRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long): RowSet =
    getOperation(opHandle).getNextRowSet(orientation, maxRows)

  @throws[KyuubiSQLException]
  def getOperationLogRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long): RowSet = {
    // get the OperationLog object from the operation
    val opLog: OperationLog = getOperation(opHandle).getOperationLog
    if (opLog == null) {
      throw new KyuubiSQLException(
        "Couldn't find log associated with operation handle: " + opHandle)
    }
    try {
      // convert logs to RowBasedSet
      val logs = opLog.readOperationLog(isFetchFirst(orientation), maxRows).asScala.map(Row(_))
      RowSetBuilder.create(logSchema, logs, getOperation(opHandle).getProtocolVersion)
    } catch {
      case e: SQLException =>
        throw new KyuubiSQLException(e.getMessage, e.getCause)
    }
  }

  def getResultSetSchema(opHandle: OperationHandle): StructType = {
    getOperation(opHandle).getResultSetSchema
  }

  private def isFetchFirst(fetchOrientation: FetchOrientation): Boolean = {
    fetchOrientation == FetchOrientation.FETCH_FIRST
  }

  def removeExpiredOperations(handles: Seq[OperationHandle]): Seq[KyuubiOperation] = {
    handles.flatMap(removeTimedOutOperation).map { op =>
      warn("Operation " + op.getHandle + " is timed-out and will be closed")
      op
    }
  }
}
