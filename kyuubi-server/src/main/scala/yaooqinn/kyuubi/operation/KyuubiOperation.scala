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

import java.io.{File, FileNotFoundException}
import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.{Future, RejectedExecutionException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.session.KyuubiSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class KyuubiOperation(session: KyuubiSession, statement: String) extends Logging {

  private[this] var state: OperationState = INITIALIZED
  private[this] val opHandle: OperationHandle =
    new OperationHandle(EXECUTE_STATEMENT, session.getProtocolVersion)

  private[this] val conf = session.sparkSession.conf

  private[this] val operationTimeout =
    KyuubiSparkUtil.timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT.key))
  private[this] var lastAccessTime = System.currentTimeMillis()

  private[this] var hasResultSet: Boolean = false
  private[this] var operationException: KyuubiSQLException = _
  private[this] var backgroundHandle: Future[_] = _
  private[this] var operationLog: OperationLog = _
  private[this] var isOperationLogEnabled: Boolean = false

  private[this] var result: DataFrame = _
  private[this] var iter: Iterator[Row] = _
  private[this] var statementId: String = _

  private[this] val DEFAULT_FETCH_ORIENTATION_SET: Set[FetchOrientation] =
    Set(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST)

  private[this] val incrementalCollect: Boolean =
    conf.get(OPERATION_INCREMENTAL_COLLECT.key).toBoolean

  def getBackgroundHandle: Future[_] = backgroundHandle

  def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    this.backgroundHandle = backgroundHandle
  }

  def getSession: KyuubiSession = session

  def getHandle: OperationHandle = opHandle

  def getProtocolVersion: TProtocolVersion = opHandle.getProtocolVersion

  def getStatus: OperationStatus = new OperationStatus(state, operationException)

  def getOperationLog: OperationLog = operationLog

  private[this] def setOperationException(opEx: KyuubiSQLException): Unit = {
    this.operationException = opEx
  }

  @throws[KyuubiSQLException]
  private[this] def setState(newState: OperationState): Unit = {
    state.validateTransition(newState)
    this.state = newState
    this.lastAccessTime = System.currentTimeMillis()
  }

  private[this] def checkState(state: OperationState): Boolean = {
    this.state == state
  }
  
  def isClosedOrCanceled: Boolean = {
    checkState(CLOSED) || checkState(CANCELED)
  }

  @throws[KyuubiSQLException]
  private[this] def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new KyuubiSQLException("Expected state " + state + ", but found " + this.state)
    }
    this.lastAccessTime = System.currentTimeMillis()
  }

  private[this] def createOperationLog(): Unit = {
    if (session.isOperationLogEnabled) {
      val logFile =
        new File(session.getSessionLogDir, opHandle.getHandleIdentifier.toString)
      val logFilePath = logFile.getAbsolutePath
      this.isOperationLogEnabled = true
      // create log file
      try {
        if (logFile.exists) {
          warn(
            s"""
               |The operation log file should not exist, but it is already there: $logFilePath"
             """.stripMargin)
          logFile.delete
        }
        if (!logFile.createNewFile) {
          // the log file already exists and cannot be deleted.
          // If it can be read/written, keep its contents and use it.
          if (!logFile.canRead || !logFile.canWrite) {
            warn(
              s"""
                 |The already existed operation log file cannot be recreated,
                 |and it cannot be read or written: $logFilePath"
               """.stripMargin)
            this.isOperationLogEnabled = false
            return
          }
        }
      } catch {
        case e: Exception =>
          warn("Unable to create operation log file: " + logFilePath, e)
          this.isOperationLogEnabled = false
          return
      }
      // create OperationLog object with above log file
      try {
        this.operationLog = new OperationLog(this.opHandle.toString, logFile, new HiveConf())
      } catch {
        case e: FileNotFoundException =>
          warn("Unable to instantiate OperationLog object for operation: " + this.opHandle, e)
          this.isOperationLogEnabled = false
          return
      }
      // register this operationLog
      session.getSessionMgr.getOperationMgr
        .setOperationLog(session.getUserName, this.operationLog)
    }
  }

  private[this] def registerCurrentOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        warn("Failed to get current OperationLog object of Operation: "
          + getHandle.getHandleIdentifier)
        isOperationLogEnabled = false
      } else {
        session.getSessionMgr.getOperationMgr
          .setOperationLog(session.getUserName, operationLog)
      }
    }
  }

  private[this] def unregisterOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      session.getSessionMgr.getOperationMgr
        .unregisterOperationLog(session.getUserName)
    }
  }

  @throws[KyuubiSQLException]
  def run(): Unit = {
    createOperationLog()
    try {
      runInternal()
    } finally {
      unregisterOperationLog()
    }
  }

  private[this] def cleanupOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        error("Operation [ " + opHandle.getHandleIdentifier + " ] " +
          "logging is enabled, but its OperationLog object cannot be found.")
      } else {
        operationLog.close()
      }
    }
  }

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    debug(s"CLOSING $statementId")
    cleanup(CLOSED)
    cleanupOperationLog()
    session.sparkSession.sparkContext.clearJobGroup()
  }

  def cancel(): Unit = {
    info(s"Cancel '$statement' with $statementId")
    cleanup(CANCELED)
  }

  def getResultSetSchema: StructType = if (result == null || result.schema.isEmpty) {
    new StructType().add("Result", "string")
  } else {
    result.schema
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(FINISHED)
    setHasResultSet(true)
    val taken = if (order == FetchOrientation.FETCH_FIRST) {
      result.toLocalIterator().asScala.take(maxRowsL.toInt)
    } else {
      iter.take(maxRowsL.toInt)
    }
    RowSetBuilder.create(getResultSetSchema, taken.toSeq, session.getProtocolVersion)
  }

  private[this] def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
    opHandle.setHasResultSet(hasResultSet)
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   */
  @throws[KyuubiSQLException]
  private[this] def validateDefaultFetchOrientation(orientation: FetchOrientation): Unit = {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET)
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   */
  @throws[KyuubiSQLException]
  private[this] def validateFetchOrientation(
      orientation: FetchOrientation,
      supportedOrientations: Set[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw new KyuubiSQLException(
        "The fetch type " + orientation.toString + " is not supported for this resultset", "HY106")
    }
  }

  private[this] def runInternal(): Unit = {
    setState(PENDING)
    setHasResultSet(true)

    // Runnable impl to call runInternal asynchronously, from a different thread
    val backgroundOperation = new Runnable() {
      override def run(): Unit = {
        try {
          session.ugi.doAs(new PrivilegedExceptionAction[Unit]() {
            registerCurrentOperationLog()
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: KyuubiSQLException => setOperationException(e)
              }
            }
          })
        } catch {
          case e: Exception => setOperationException(new KyuubiSQLException(e))
        }
      }
    }

    try {
      // This submit blocks if no background threads are available to run this operation
      val backgroundHandle =
        session.getSessionMgr.submitBackgroundOperation(backgroundOperation)
      setBackgroundHandle(backgroundHandle)
    } catch {
      case rejected: RejectedExecutionException =>
        setState(ERROR)
        throw new KyuubiSQLException("The background threadpool cannot accept" +
          " new task for execution, please retry the operation", rejected)
      case NonFatal(e) =>
        error(s"Error executing query in background", e)
        setState(ERROR)
        throw e
    }
  }

  private[this] def execute(): Unit = {
    try {
      statementId = UUID.randomUUID().toString
      info(s"Running query '$statement' with $statementId")
      setState(RUNNING)
      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementStart(
          statementId,
          session.getSessionHandle.getSessionId.toString,
          statement,
          statementId,
          session.getUserName)
      }
      session.sparkSession.sparkContext.setJobGroup(statementId, statement)
      result = session.sparkSession.sql(statement)
      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementParsed(statementId, result.queryExecution.toString())
      }
      debug(result.queryExecution.toString())
      iter = if (incrementalCollect) {
        info("Executing query in incremental collection mode")
        result.toLocalIterator().asScala
      } else {
        result.collect().toList.iterator
      }
      setState(FINISHED)
      KyuubiServerMonitor.getListener(session.getUserName).foreach(_.onStatementFinish(statementId))
    } catch {
      case e: KyuubiSQLException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
          throw e
        }
      case e: ParseException =>
        if (!isClosedOrCanceled) {
          onStatementError(
            statementId, e.withCommand(statement).getMessage, KyuubiSparkUtil.exceptionString(e))
          throw new KyuubiSQLException(
            e.withCommand(statement).getMessage, "ParseException", 2000, e)
        }
      case e: AnalysisException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
          throw new KyuubiSQLException(e.getMessage, "AnalysisException", 2001, e)
        }
      case e: HiveAccessControlException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
          throw new KyuubiSQLException(e.getMessage, "HiveAccessControlException", 3000, e)
        }
      case e: Throwable =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
          throw new KyuubiSQLException(e.toString, "<unknown>", 10000, e)
        }
    } finally {
      if (statementId != null) {
        session.sparkSession.sparkContext.cancelJobGroup(statementId)
      }
    }
  }

  private[this] def onStatementError(id: String, message: String, trace: String): Unit = {
    error(
      s"""
         |Error executing query as ${session.getUserName},
         |$statement
         |Current operation state ${this.state},
         |$trace
       """.stripMargin)
    setState(ERROR)
    KyuubiServerMonitor.getListener(session.getUserName)
      .foreach(_.onStatementError(id, message, trace))
  }

  private[this] def cleanup(state: OperationState) {
    if (this.state != CLOSED) {
      setState(state)
    }
    val backgroundHandle = getBackgroundHandle
    if (backgroundHandle != null) {
      backgroundHandle.cancel(true)
    }
    if (statementId != null) {
      session.sparkSession.sparkContext.cancelJobGroup(statementId)
    }
  }

  def isTimedOut: Boolean = {
    if (operationTimeout <= 0) {
      false
    } else {
      // check only when it's in terminal state
      state.isTerminal && lastAccessTime + operationTimeout <= System.currentTimeMillis()
    }
  }
}

object KyuubiOperation {
  val DEFAULT_FETCH_ORIENTATION: FetchOrientation = FetchOrientation.FETCH_NEXT
  val DEFAULT_FETCH_MAX_ROWS = 100
}
