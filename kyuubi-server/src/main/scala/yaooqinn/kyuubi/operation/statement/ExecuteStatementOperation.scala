/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.operation.statement

import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.RejectedExecutionException

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.operation._
import yaooqinn.kyuubi.session.KyuubiSession

/**
 * Abstract class for executing statement with SQL queries
 * @param session Parent [[KyuubiSession]]
 * @param statement sql statement
 */
abstract class ExecuteStatementOperation(
    session: KyuubiSession,
    statement: String,
    runAsync: Boolean)
  extends AbstractOperation(session, EXECUTE_STATEMENT) {

  protected val statementId: String = UUID.randomUUID().toString

  protected def execute(): Unit

  protected def onStatementError(id: String, message: String, trace: String): Unit = {
    error(
      s"""
         |Error executing query as ${session.getUserName},
         |$statement
         |Current operation state ${getStatus.getState},
         |$trace
       """.stripMargin)
    setState(ERROR)
  }

  protected def cleanup(state: OperationState): Unit = {
    setState(state)
    if (shouldRunAsync) {
      val backgroundHandle = getBackgroundHandle
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
  }

  override protected def runInternal(): Unit = {
    setState(PENDING)
    setHasResultSet(true)
    val task = new Runnable() {
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

    if (shouldRunAsync) {
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle = session.getSessionMgr.submitBackgroundOperation(task)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(ERROR)
          throw new KyuubiSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
      }
    } else {
      task.run()
    }
  }

  private def registerCurrentOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        warn("Failed to get current OperationLog object of Operation: " +
          getHandle.getHandleIdentifier)
        isOperationLogEnabled = false
      } else {
        session.getSessionMgr.getOperationMgr
          .setOperationLog(session.getUserName, operationLog)
      }
    }
  }

  override def shouldRunAsync: Boolean = runAsync

}
