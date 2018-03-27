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
import org.apache.hive.service.cli._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkUtils}
import org.apache.spark.KyuubiConf._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.service.AbstractService
import yaooqinn.kyuubi.session.KyuubiSession

private[kyuubi] class OperationManager private(name: String)
  extends AbstractService(name) with Logging {

  def this() = this(classOf[OperationManager].getSimpleName)

  private[this] val handleToOperation = new ConcurrentHashMap[OperationHandle, KyuubiOperation]

  private[this] val userToOperationLog = new ConcurrentHashMap[String, OperationLog]()

  override def init(conf: SparkConf): Unit = synchronized {
    if (conf.get(LOGGING_OPERATION_ENABLED.key).toBoolean) {
      initOperationLogCapture()
    } else {
      debug("Operation level logging is turned off")
    }
    super.init(conf)
  }

  private[this] def initOperationLogCapture(): Unit = {
    // Register another Appender (with the same layout) that talks to us.
    val ap = new LogDivertAppender(this)
    Logger.getRootLogger.addAppender(ap)
  }

  private[this] def getOperationLogByThread: OperationLog = OperationLog.getCurrentOperationLog

  private[this] def getOperationLogByName: OperationLog = {
    if (!userToOperationLog.isEmpty) {
      userToOperationLog.get(SparkUtils.getCurrentUserName())
    } else {
      null
    }
  }

  def getOperationLog: OperationLog = {
    Option(getOperationLogByThread).getOrElse(getOperationLogByName)
  }

  def setOperationLog(user: String, log: OperationLog): Unit = {
    OperationLog.setCurrentOperationLog(log)
    userToOperationLog.put(Option(user).getOrElse(SparkUtils.getCurrentUserName()), log)
  }

  def unregisterOperationLog(user: String): Unit = {
    OperationLog.removeCurrentOperationLog()
    userToOperationLog.remove(user)
  }

  def newExecuteStatementOperation(
      parentSession: KyuubiSession,
      statement: String): KyuubiOperation = synchronized {
    val operation = new KyuubiOperation(parentSession, statement)
    addOperation(operation)
    operation
  }

  def getOperation(operationHandle: OperationHandle): KyuubiOperation = {
    val operation = getOperationInternal(operationHandle)
    if (operation == null) {
      throw new KyuubiSQLException("Invalid OperationHandle: " + operationHandle)
    }
    operation
  }

  private[this] def getOperationInternal(operationHandle: OperationHandle) =
    handleToOperation.get(operationHandle)

  private[this] def addOperation(operation: KyuubiOperation): Unit = {
    handleToOperation.put(operation.getHandle, operation)
  }

  private[this] def removeOperation(opHandle: OperationHandle) =
    handleToOperation.remove(opHandle)

  private def removeTimedOutOperation(
      operationHandle: OperationHandle): Option[KyuubiOperation] = synchronized {
    Some(handleToOperation.get(operationHandle))
      .filter(_.isTimedOut)
      .map(_ => handleToOperation.remove(operationHandle))
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
      throw new KyuubiSQLException("Couldn't find log associated with operation handle: " + opHandle)
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

  private[this] def isFetchFirst(fetchOrientation: FetchOrientation): Boolean = {
    fetchOrientation == FetchOrientation.FETCH_FIRST
  }

  private[this] def logSchema: StructType = new StructType().add("operation_log", "string")

  def removeExpiredOperations(handles: Seq[OperationHandle]): Seq[KyuubiOperation] = {
    handles.flatMap(removeTimedOutOperation).map { op =>
      warn("Operation " + op.getHandle + " is timed-out and will be closed")
      op
    }
  }
}
