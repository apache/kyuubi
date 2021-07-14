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

import java.util.concurrent.{RejectedExecutionException, ScheduledExecutorService, TimeUnit}

import scala.collection.mutable.Map

import org.apache.spark.kyuubi.SQLOperationListener
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.{ArrayFetchIterator, KyuubiSparkUtil}
import org.apache.kyuubi.engine.spark.monitor.KyuubiStatementMonitor
import org.apache.kyuubi.engine.spark.monitor.entity.KyuubiStatementInfo
import org.apache.kyuubi.operation.{OperationState, OperationType}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThreadUtils

class ExecuteStatement(
    spark: SparkSession,
    session: Session,
    protected override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long)
  extends SparkOperation(spark, OperationType.EXECUTE_STATEMENT, session) with Logging {

  import ExecuteStatement._

  private val forceCancel =
    session.sessionManager.getConf.get(KyuubiConf.OPERATION_FORCE_CANCEL)

  private val schedulerPool =
    spark.conf.getOption(KyuubiConf.OPERATION_SCHEDULER_POOL.key).orElse(
      session.sessionManager.getConf.get(KyuubiConf.OPERATION_SCHEDULER_POOL))

  private var statementTimeoutCleaner: Option[ScheduledExecutorService] = None

  private val operationLog: OperationLog =
    OperationLog.createOperationLog(session.handle, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)
  private var result: DataFrame = _

  private val operationListener: SQLOperationListener = new SQLOperationListener(this, spark)

  private val kyuubiStatementInfo = KyuubiStatementInfo(
    statementId, statement, spark.sparkContext.applicationId,
    session.getTypeInfo.identifier, null, null, Map(state -> lastAccessTime))
  KyuubiStatementMonitor.putStatementInfoIntoQueue(kyuubiStatementInfo)

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

  private def executeStatement(): Unit = withLocalProperties {
    try {
      setState(OperationState.RUNNING)
      info(KyuubiSparkUtil.diagnostics)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      // TODO: Make it configurable
      spark.sparkContext.addSparkListener(operationListener)
      result = spark.sql(statement)
      kyuubiStatementInfo.queryExecution = result.queryExecution
      debug(result.queryExecution)
      iter = new ArrayFetchIterator(result.collect())
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      statementTimeoutCleaner.foreach(_.shutdown())
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

  private def withLocalProperties[T](f: => T): T = {
    try {
      spark.sparkContext.setJobGroup(statementId, statement, forceCancel)
      spark.sparkContext.setLocalProperty(KYUUBI_STATEMENT_ID_KEY, statementId)
      schedulerPool match {
        case Some(pool) =>
          spark.sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL_KEY, pool)
        case None =>
      }

      f
    } finally {
      spark.sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL_KEY, null)
      spark.sparkContext.setLocalProperty(KYUUBI_STATEMENT_ID_KEY, null)
      spark.sparkContext.clearJobGroup()
    }
  }

  private def addTimeoutMonitor(): Unit = {
    if (queryTimeout > 0) {
      val timeoutExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("query-timeout-thread")
      timeoutExecutor.schedule(new Runnable {
        override def run(): Unit = {
          cleanup(OperationState.TIMEOUT)
        }
      }, queryTimeout, TimeUnit.SECONDS)
      statementTimeoutCleaner = Some(timeoutExecutor)
    }
  }

  override def cleanup(targetState: OperationState): Unit = {
    spark.sparkContext.removeSparkListener(operationListener)
    super.cleanup(targetState)
  }

  override def setState(newState: OperationState): Unit = {
    super.setState(newState)
    kyuubiStatementInfo.stateToTime.put(newState, lastAccessTime)
  }

  override def setOperationException(opEx: KyuubiSQLException): Unit = {
    super.setOperationException(opEx)
    kyuubiStatementInfo.exception = opEx
  }
}

object ExecuteStatement {
  final val KYUUBI_STATEMENT_ID_KEY = "kyuubi.statement.id"
  final val SPARK_SCHEDULER_POOL_KEY = "spark.scheduler.pool"
  final val SPARK_SQL_EXECUTION_ID_KEY = "spark.sql.execution.id"
}
