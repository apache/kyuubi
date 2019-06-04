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
import java.util.concurrent.Future

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.session.KyuubiSession

abstract class AbstractOperation(
    session: KyuubiSession,
    opType: OperationType,
    runAsync: Boolean) extends KyuubiOperation with Logging {
  private var state: OperationState = INITIALIZED
  private val opHandle: OperationHandle = new OperationHandle(opType, session.getProtocolVersion)
  protected val conf: SparkConf = session.getConf
  protected var iter: Iterator[Row] = _
  protected var hasResultSet: Boolean = false
  protected val operationTimeout: Long = timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT))
  protected var lastAccessTime = System.currentTimeMillis()
  @volatile protected var operationException: KyuubiSQLException = _
  @volatile protected var backgroundHandle: Future[_] = _
  protected var operationLog: OperationLog = _
  protected var isOperationLogEnabled: Boolean = false

  protected val DEFAULT_FETCH_ORIENTATION_SET: Set[FetchOrientation] =
    Set(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST)

  def getBackgroundHandle: Future[_] = backgroundHandle

  def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    this.backgroundHandle = backgroundHandle
  }

  override def shouldRunAsync: Boolean = runAsync

  override def getSession: KyuubiSession = session

  override def getHandle: OperationHandle = opHandle

  override def getProtocolVersion: TProtocolVersion = opHandle.getProtocolVersion

  override def getStatus: OperationStatus = new OperationStatus(state, operationException)

  override def getOperationLog: OperationLog = operationLog

  override def isTimedOut: Boolean = {
    if (operationTimeout <= 0) {
      false
    } else {
      // check only when it's in terminal state
      state.isTerminal && lastAccessTime + operationTimeout <= System.currentTimeMillis()
    }
  }

  override def cancel(): Unit = {
    setState(CANCELED)
    throw new UnsupportedOperationException("KyuubiOperation.cancel()")
  }

  protected def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
    opHandle.setHasResultSet(hasResultSet)
  }

  protected def setOperationException(opEx: KyuubiSQLException): Unit = {
    this.operationException = opEx
  }

  @throws[KyuubiSQLException]
  protected def setState(newState: OperationState): Unit = {
    state.validateTransition(newState)
    this.state = newState
    this.lastAccessTime = System.currentTimeMillis()
  }

  protected def checkState(state: OperationState): Boolean = {
    this.state == state
  }

  protected def isClosedOrCanceled: Boolean = {
    checkState(CLOSED) || checkState(CANCELED)
  }

  @throws[KyuubiSQLException]
  protected def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new KyuubiSQLException("Expected state " + state + ", but found " + this.state)
    }
    this.lastAccessTime = System.currentTimeMillis()
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   */
  @throws[KyuubiSQLException]
  protected def validateDefaultFetchOrientation(orientation: FetchOrientation): Unit = {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET)
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   */
  private def validateFetchOrientation(
      orientation: FetchOrientation,
      supportedOrientations: Set[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw new KyuubiSQLException(
        "The fetch type " + orientation.toString + " is not supported for this resultset", "HY106")
    }
  }

  private def createOperationLog(): Unit = {
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
      session.getSessionMgr.getOperationMgr.setOperationLog(session.getUserName, this.operationLog)
    }
  }

  private def unregisterOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      session.getSessionMgr.getOperationMgr.unregisterOperationLog(session.getUserName)
    }
  }

  protected def cleanupOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        error("Operation [ " + opHandle.getHandleIdentifier + " ] " +
          "logging is enabled, but its OperationLog object cannot be found.")
      } else {
        operationLog.close()
      }
    }
  }

  /**
   * Invoked before runInternal() to setup some preconditions.
   */
  protected def beforeRun(): Unit = {
    createOperationLog()
  }

  /**
   * Invoked after runInternal() to clean up resources, which was set up in beforeRun().
   */
  protected def afterRun(): Unit = {
    unregisterOperationLog()
  }

  /**
   * Implemented by subclasses to decide how to execute specific behavior.
   */
  protected def runInternal(): Unit

  override def run(): Unit = {
    beforeRun()
    try {
      runInternal()
    } finally {
      afterRun()
    }
  }
}
