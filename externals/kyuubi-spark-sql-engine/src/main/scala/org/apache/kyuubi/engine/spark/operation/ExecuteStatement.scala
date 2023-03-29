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

import java.util.concurrent.RejectedExecutionException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{CollectLimitExec, SQLExecution}
import org.apache.spark.sql.execution.arrow.{ArrowCollectLimitExec, KyuubiArrowUtils}
import org.apache.spark.sql.kyuubi.SparkDatasetHelper
import org.apache.spark.sql.types._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf.OPERATION_RESULT_MAX_ROWS
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.operation.{ArrayFetchIterator, FetchIterator, IterableFetchIterator, OperationHandle, OperationState}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean,
    override protected val handle: OperationHandle)
  extends SparkOperation(session) with Logging {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)
  override protected def supportProgress: Boolean = true

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

  protected def incrementalCollectResult(resultDF: DataFrame): Iterator[Any] = {
    resultDF.toLocalIterator().asScala
  }

  protected def fullCollectResult(resultDF: DataFrame): Array[_] = {
    resultDF.collect()
  }

  protected def takeResult(resultDF: DataFrame, maxRows: Int): Array[_] = {
    resultDF.take(maxRows)
  }

  protected def executeStatement(): Unit = withLocalProperties {
    try {
      setState(OperationState.RUNNING)
      info(diagnostics)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      addOperationListener()
      result = spark.sql(statement)
      iter = collectAsIterator(result)
      setCompiledStateIfNeeded()
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      shutdownTimeoutMonitor()
    }
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
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
          val ke =
            KyuubiSQLException("Error submitting query in background, query rejected", rejected)
          setOperationException(ke)
          throw ke
      }
    } else {
      executeStatement()
    }
  }

  def setCompiledStateIfNeeded(): Unit = synchronized {
    if (getStatus.state == OperationState.RUNNING) {
      val lastAccessCompiledTime =
        if (result != null) {
          val phase = result.queryExecution.tracker.phases
          if (phase.contains("parsing") && phase.contains("planning")) {
            val compiledTime = phase("planning").endTimeMs - phase("parsing").startTimeMs
            lastAccessTime + compiledTime
          } else {
            0L
          }
        } else {
          0L
        }
      setState(OperationState.COMPILED)
      if (lastAccessCompiledTime > 0L) {
        lastAccessTime = lastAccessCompiledTime
      }
    }
  }

  override def getResultSetMetadataHints(): Seq[String] =
    Seq(
      s"__kyuubi_operation_result_format__=$resultFormat",
      s"__kyuubi_operation_result_arrow_timestampAsString__=$timestampAsString")

  private def collectAsIterator(resultDF: DataFrame): FetchIterator[_] = {
    val resultMaxRows = spark.conf.getOption(OPERATION_RESULT_MAX_ROWS.key).map(_.toInt)
      .getOrElse(session.sessionManager.getConf.get(OPERATION_RESULT_MAX_ROWS))
    if (incrementalCollect) {
      if (resultMaxRows > 0) {
        warn(s"Ignore ${OPERATION_RESULT_MAX_ROWS.key} on incremental collect mode.")
      }
      info("Execute in incremental collect mode")
      new IterableFetchIterator[Any](new Iterable[Any] {
        override def iterator: Iterator[Any] = incrementalCollectResult(resultDF)
      })
    } else {
      val internalArray = if (resultMaxRows <= 0) {
        info("Execute in full collect mode")
        fullCollectResult(resultDF)
      } else {
        info(s"Execute with max result rows[$resultMaxRows]")
        takeResult(resultDF, resultMaxRows)
      }
      new ArrayFetchIterator(internalArray)
    }
  }
}

class ArrowBasedExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean,
    override protected val handle: OperationHandle)
  extends ExecuteStatement(
    session,
    statement,
    shouldRunAsync,
    queryTimeout,
    incrementalCollect,
    handle) {

  override protected def incrementalCollectResult(resultDF: DataFrame): Iterator[Any] = {
    val df = convertComplexType(resultDF)
    withNewExecutionId(df) {
      SparkDatasetHelper.toArrowBatchRdd(df).toLocalIterator
    }
  }

  override protected def fullCollectResult(resultDF: DataFrame): Array[_] = {
    executeCollect(convertComplexType(resultDF))
  }

  override protected def takeResult(resultDF: DataFrame, maxRows: Int): Array[_] = {
    executeCollect(convertComplexType(resultDF.limit(maxRows)))
  }

  /**
   * refer to org.apache.spark.sql.Dataset#withAction(), assign a new execution id for arrow-based
   * operation, so that we can track the arrow-based queries on the UI tab.
   */
  private def withNewExecutionId[T](df: DataFrame)(body: => T): T = {
    SQLExecution.withNewExecutionId(df.queryExecution, Some("collectAsArrow")) {
      df.queryExecution.executedPlan.resetMetrics()
      body
    }
  }

  def executeCollect(df: DataFrame): Array[Array[Byte]] = withNewExecutionId(df) {
    executeArrowBatchCollect(df).getOrElse {
      SparkDatasetHelper.toArrowBatchRdd(df).collect()
    }
  }

  private def executeArrowBatchCollect(df: DataFrame): Option[Array[Array[Byte]]] = {
    df.queryExecution.executedPlan match {
      case collectLimit @ CollectLimitExec(limit, _) =>
        val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone
        val maxRecordsPerBatch = spark.conf.getOption(
          "spark.sql.execution.arrow.maxRecordsPerBatch").map(_.toInt).getOrElse(10000)
        // val maxBatchSize =
        // (spark.sessionState.conf.getConf(SPARK_CONNECT_GRPC_ARROW_MAX_BATCH_SIZE) * 0.7).toLong
        val maxBatchSize = 1024 * 1024 * 4
        val batches = ArrowCollectLimitExec.takeAsArrowBatches(
          collectLimit,
          df.schema,
          maxRecordsPerBatch,
          maxBatchSize,
          timeZoneId)
        val result = ArrayBuffer[Array[Byte]]()
        var i = 0
        var rest = limit
        while (i < batches.length && rest > 0) {
          val (batch, size) = batches(i)
          if (size <= rest) {
            result += batch
            // returned ArrowRecordBatch has less than `limit` row count, safety to do conversion
            rest -= size.toInt
          } else { // size > rest
            result += KyuubiArrowUtils.sliceV2(df.schema, timeZoneId, batch, 0, rest)
            rest = 0
          }
          i += 1
        }
        Option(result.toArray)
      case _ =>
        None
    }
  }

  override protected def isArrowBasedOperation: Boolean = true

  override val resultFormat = "arrow"

  private def convertComplexType(df: DataFrame): DataFrame = {
    SparkDatasetHelper.convertTopLevelComplexTypeToHiveString(df, timestampAsString)
  }

}
