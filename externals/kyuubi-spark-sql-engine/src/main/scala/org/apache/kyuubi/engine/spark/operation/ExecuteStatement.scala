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

package org.apache.kyuubi.engine.spark.operation

import java.util.concurrent.{Executors, RejectedExecutionException, TimeUnit}

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.spark.{ArrayFetchIterator, Constants, KyuubiSparkUtil}
import org.apache.kyuubi.operation.{OperationState, OperationType}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    spark: SparkSession,
    session: Session,
    protected override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long)
  extends SparkOperation(spark, OperationType.EXECUTE_STATEMENT, session) with Logging {

  // If a timeout value `queryTimeout` is specified by users and it is smaller than
  // a global timeout value, we use the user-specified value.
  // This code follows the Hive timeout behaviour (See #29933 for details).
  private val timeout = {
    val globalTimeout =
      spark.sessionState.conf.getConfString(Constants.THRIFTSERVER_QUERY_TIMEOUT, "0").toInt
    if (globalTimeout > 0 && (queryTimeout <= 0 || globalTimeout < queryTimeout)) {
      globalTimeout
    } else {
      queryTimeout
    }
  }
  private val forceCancel =
    spark.sessionState.conf.getConfString(Constants.THRIFTSERVER_FORCE_CANCEL, "true").toBoolean

  private val operationLog: OperationLog =
    OperationLog.createOperationLog(session.handle, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)
  private var result: DataFrame = _

  override protected def resultSchema: StructType = {
    if (result == null || result.schema.isEmpty) {
      new StructType().add("Result", "string")
    } else {
      result.schema
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  private def executeStatement(): Unit = {
    try {
      setState(OperationState.RUNNING)
      info(KyuubiSparkUtil.diagnostics(spark))
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      spark.sparkContext.setJobGroup(statementId, statement, forceCancel)
      result = spark.sql(statement)
      debug(result.queryExecution)
      iter = new ArrayFetchIterator(result.collect())
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      spark.sparkContext.clearJobGroup()
    }
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor()
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          OperationLog.setCurrentOperationLog(operationLog)
          executeStatement()
        }
      }

      try {
        val sparkSQLSessionManager = session.sessionManager
        val backgroundHandle = sparkSQLSessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException("Error submitting query in background, query rejected",
            rejected)
          setOperationException(ke)
          throw ke
      }
    } else {
      executeStatement()
    }
  }

  private def addTimeoutMonitor(): Unit = {
    if (timeout > 0) {
      val timeoutExecutor = Executors.newSingleThreadScheduledExecutor()
      timeoutExecutor.schedule(new Runnable {
        override def run(): Unit = {
          try {
            if (getStatus.state != OperationState.TIMEOUT) {
              info(s"Query with $statementId timed out after $timeout seconds")
              cleanup(OperationState.TIMEOUT)
            }
          } catch {
            case NonFatal(e) =>
              setOperationException(KyuubiSQLException(e))
              error(s"Error cancelling the query after timeout: $timeout seconds")
          } finally {
            timeoutExecutor.shutdown()
          }
        }
      }, timeout, TimeUnit.SECONDS)
    }
  }
}
