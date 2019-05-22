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

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{Future, RejectedExecutionException}

import scala.util.control.NonFatal

import org.apache.hive.service.cli.thrift.TProtocolVersion

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.session.KyuubiSession

abstract class AbstractKyuubiOperation(session: KyuubiSession, statement: String)
  extends IKyuubiOperation with Logging{

  protected var state: OperationState = INITIALIZED
  protected val opHandle: OperationHandle =
    new OperationHandle(EXECUTE_STATEMENT, session.getProtocolVersion)
  protected val operationTimeout: Long
  protected var lastAccessTime = System.currentTimeMillis()

  protected var hasResultSet: Boolean = false
  protected var operationException: KyuubiSQLException = _
  protected var backgroundHandle: Future[_] = _
  protected var statementId: String = _

  protected val DEFAULT_FETCH_ORIENTATION_SET: Set[FetchOrientation] =
    Set(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST)

  def getBackgroundHandle: Future[_] = backgroundHandle

  def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    this.backgroundHandle = backgroundHandle
  }

  override def getSession: KyuubiSession = session

  override def getHandle: OperationHandle = opHandle

  override def getProtocolVersion: TProtocolVersion = opHandle.getProtocolVersion

  override def getStatus: OperationStatus = new OperationStatus(state, operationException)

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

  override def isClosedOrCanceled: Boolean = {
    checkState(CLOSED) || checkState(CANCELED)
  }

  @throws[KyuubiSQLException]
  protected def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new KyuubiSQLException("Expected state " + state + ", but found " + this.state)
    }
    this.lastAccessTime = System.currentTimeMillis()
  }

  override def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    debug(s"CLOSING $statementId")
    cleanup(CLOSED)
  }

  override def cancel(): Unit = {
    info(s"Cancel '$statement' with $statementId")
    cleanup(CANCELED)
  }

  protected def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
    opHandle.setHasResultSet(hasResultSet)
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
  @throws[KyuubiSQLException]
  protected def validateFetchOrientation(
      orientation: FetchOrientation,
      supportedOrientations: Set[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw new KyuubiSQLException(
        "The fetch type " + orientation.toString + " is not supported for this resultset", "HY106")
    }
  }

  protected def runInternal(): Unit = {
    setState(PENDING)
    setHasResultSet(true)

    // Runnable impl to call runInternal asynchronously, from a different thread
    val backgroundOperation = new Runnable() {
      override def run(): Unit = {
        try {
          session.ugi.doAs(new PrivilegedExceptionAction[Unit]() {
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
    }
  }

  protected def execute(): Unit

  protected def onStatementError(id: String, message: String, trace: String): Unit = {
    error(
      s"""
         |Error executing query as ${session.getUserName},
         |$statement
         |Current operation state ${this.state},
         |$trace
       """.stripMargin)
    setState(ERROR)
  }

  protected def cleanup(state: OperationState) {
    if (this.state != CLOSED) {
      setState(state)
    }
    val backgroundHandle = getBackgroundHandle
    if (backgroundHandle != null) {
      backgroundHandle.cancel(true)
    }
  }

  override def isTimedOut: Boolean = {
    if (operationTimeout <= 0) {
      false
    } else {
      // check only when it's in terminal state
      state.isTerminal && lastAccessTime + operationTimeout <= System.currentTimeMillis()
    }
  }

}
