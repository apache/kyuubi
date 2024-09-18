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

import scala.Array._
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.kyuubi.SparkDatasetHelper._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SPARK_OPERATION_INCREMENTAL_COLLECT_CANCEL_JOB_GROUP, OPERATION_RESULT_MAX_ROWS, OPERATION_RESULT_SAVE_TO_FILE, OPERATION_RESULT_SAVE_TO_FILE_MIN_ROWS, OPERATION_RESULT_SAVE_TO_FILE_MINSIZE}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.engine.spark.session.{SparkSessionImpl, SparkSQLSessionManager}
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

  private var fetchOrcStatement: Option[FetchOrcStatement] = None
  private var saveFilePath: Option[Path] = None

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override def close(): Unit = {
    super.close()
    fetchOrcStatement.foreach(_.close())
    saveFilePath.foreach { p =>
      p.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(p, true)
    }
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

  protected def executeStatement(): Unit =
    try {
      withLocalProperties {
        setState(OperationState.RUNNING)
        info(diagnostics)
        Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
        addOperationListener()
        result = spark.sql(statement)
        iter = collectAsIterator(result)
        setCompiledStateIfNeeded()
        setState(OperationState.FINISHED)
      }
    } catch {
      onError(cancel = true)
    } finally {
      shutdownTimeoutMonitor()
      if (!spark.sparkContext.isStopped) {
        if (!incrementalCollect ||
          getSessionConf(ENGINE_SPARK_OPERATION_INCREMENTAL_COLLECT_CANCEL_JOB_GROUP, spark)) {
          spark.sparkContext.cancelJobGroup(statementId)
        }
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
    val resultMaxRows: Int = getSessionConf(OPERATION_RESULT_MAX_ROWS, spark)
    if (incrementalCollect) {
      if (resultMaxRows > 0) {
        warn(s"Ignore ${OPERATION_RESULT_MAX_ROWS.key} on incremental collect mode.")
      }
      info("Execute in incremental collect mode")
      new IterableFetchIterator[Any](new Iterable[Any] {
        override def iterator: Iterator[Any] = incrementalCollectResult(resultDF)
      })
    } else {
      val resultSaveEnabled = getSessionConf(OPERATION_RESULT_SAVE_TO_FILE, spark)
      val resultSaveSizeThreshold = getSessionConf(OPERATION_RESULT_SAVE_TO_FILE_MINSIZE, spark)
      val resultSaveRowsThreshold = getSessionConf(OPERATION_RESULT_SAVE_TO_FILE_MIN_ROWS, spark)
      if (hasResultSet && resultSaveEnabled && shouldSaveResultToFs(
          resultMaxRows,
          resultSaveSizeThreshold,
          resultSaveRowsThreshold,
          result)) {
        saveFilePath =
          Some(
            session.sessionManager.asInstanceOf[SparkSQLSessionManager].getOperationResultSavePath(
              session.handle,
              handle))
        // Rename all col name to avoid duplicate columns
        val colName = range(0, result.schema.size).map(x => "col" + x)

        // df.write will introduce an extra shuffle for the outermost limit, and hurt performance
        if (resultMaxRows > 0) {
          result.toDF(colName: _*).limit(resultMaxRows).write
            .option("compression", "zstd").format("orc").save(saveFilePath.get.toString)
        } else {
          result.toDF(colName: _*).write
            .option("compression", "zstd").format("orc").save(saveFilePath.get.toString)
        }
        info(s"Save result to ${saveFilePath.get}")
        fetchOrcStatement = Some(new FetchOrcStatement(spark))
        return fetchOrcStatement.get.getIterator(saveFilePath.get.toString, resultSchema)
      }
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

  checkUseLargeVarType()

  override protected def incrementalCollectResult(resultDF: DataFrame): Iterator[Any] = {
    toArrowBatchLocalIterator(convertComplexType(resultDF))
  }

  override protected def fullCollectResult(resultDF: DataFrame): Array[_] = {
    executeCollect(convertComplexType(resultDF))
  }

  override protected def takeResult(resultDF: DataFrame, maxRows: Int): Array[_] = {
    executeCollect(convertComplexType(resultDF.limit(maxRows)))
  }

  override protected def isArrowBasedOperation: Boolean = true

  override val resultFormat = "arrow"

  private def convertComplexType(df: DataFrame): DataFrame = {
    convertTopLevelComplexTypeToHiveString(df, timestampAsString)
  }

  def checkUseLargeVarType(): Unit = {
    // TODO: largeVarType support, see SPARK-39979.
    val useLargeVarType = session.asInstanceOf[SparkSessionImpl].spark
      .conf
      .get("spark.sql.execution.arrow.useLargeVarType", "false")
      .toBoolean
    if (useLargeVarType) {
      throw new KyuubiSQLException(
        "`spark.sql.execution.arrow.useLargeVarType = true` not support now.",
        null)
    }
  }
}
