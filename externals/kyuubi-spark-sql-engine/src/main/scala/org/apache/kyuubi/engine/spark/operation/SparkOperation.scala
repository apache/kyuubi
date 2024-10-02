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

import java.io.IOException
import java.time.ZoneId

import org.apache.spark.kyuubi.{SparkProgressMonitor, SQLOperationListener}
import org.apache.spark.kyuubi.SparkUtilsHelper.redact
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.ui.SparkUIUtils.formatDuration

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ARROW_BASED_ROWSET_TIMESTAMP_AS_STRING, ENGINE_SPARK_OUTPUT_MODE, EngineSparkOutputMode, OPERATION_SPARK_LISTENER_ENABLED, SESSION_PROGRESS_ENABLE, SESSION_USER_SIGN_ENABLED}
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_SIGN_PUBLICKEY, KYUUBI_SESSION_USER_KEY, KYUUBI_SESSION_USER_SIGN, KYUUBI_STATEMENT_ID_KEY}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.{getSessionConf, SPARK_SCHEDULER_POOL_KEY}
import org.apache.kyuubi.engine.spark.events.SparkOperationEvent
import org.apache.kyuubi.engine.spark.operation.SparkOperation.TIMEZONE_KEY
import org.apache.kyuubi.engine.spark.schema.{SchemaHelper, SparkArrowTRowSetGenerator, SparkTRowSetGenerator}
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{AbstractOperation, FetchIterator, OperationState, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation._
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TFetchResultsResp, TGetResultSetMetadataResp, TProgressUpdateResp, TRowSet}
import org.apache.kyuubi.util.ThriftUtils

abstract class SparkOperation(session: Session)
  extends AbstractOperation(session) {

  protected val spark: SparkSession = session.asInstanceOf[SparkSessionImpl].spark

  private val timeZone: ZoneId = {
    spark.conf.getOption(TIMEZONE_KEY).map { timeZoneId =>
      ZoneId.of(timeZoneId.replaceFirst("(\\+|\\-)(\\d):", "$10$2:"), ZoneId.SHORT_IDS)
    }.getOrElse(ZoneId.systemDefault())
  }

  protected var iter: FetchIterator[_] = _

  protected var result: DataFrame = _

  protected def resultSchema: StructType = {
    if (!hasResultSet) {
      new StructType()
    } else if (result == null || result.schema.isEmpty) {
      new StructType().add("Result", "string")
    } else {
      result.schema
    }
  }

  override def redactedStatement: String =
    redact(spark.sessionState.conf.stringRedactionPattern, statement)

  protected val operationSparkListenerEnabled: Boolean =
    getSessionConf(OPERATION_SPARK_LISTENER_ENABLED, spark)

  protected val operationListener: Option[SQLOperationListener] =
    if (operationSparkListenerEnabled) {
      Some(new SQLOperationListener(this, spark))
    } else {
      None
    }

  protected def addOperationListener(): Unit = {
    operationListener.foreach(spark.sparkContext.addSparkListener(_))
  }

  private val progressEnable: Boolean = getSessionConf(SESSION_PROGRESS_ENABLE, spark)

  protected def supportProgress: Boolean = false

  protected def outputMode: EngineSparkOutputMode.EngineSparkOutputMode =
    EngineSparkOutputMode.withName(getSessionConf(ENGINE_SPARK_OUTPUT_MODE, spark))

  override def getStatus: OperationStatus = {
    if (progressEnable && supportProgress) {
      val progressMonitor = new SparkProgressMonitor(spark, statementId)
      setOperationJobProgress(new TProgressUpdateResp(
        progressMonitor.headers,
        progressMonitor.rows,
        progressMonitor.progressedPercentage,
        progressMonitor.executionStatus,
        progressMonitor.footerSummary,
        startTime))
    }
    super.getStatus
  }

  override def cleanup(targetState: OperationState): Unit = withLockRequired {
    operationListener.foreach(_.cleanup())
    if (!isTerminalState(state)) {
      setState(targetState)
      Option(getBackgroundHandle).foreach(_.cancel(true))
    }
    if (!spark.sparkContext.isStopped) spark.sparkContext.cancelJobGroup(statementId)
  }

  protected val forceCancel =
    session.sessionManager.getConf.get(KyuubiConf.OPERATION_FORCE_CANCEL)

  protected val schedulerPool = getSessionConf(KyuubiConf.OPERATION_SCHEDULER_POOL, spark)

  protected val isSessionUserSignEnabled: Boolean = spark.sparkContext.getConf.getBoolean(
    s"spark.${SESSION_USER_SIGN_ENABLED.key}",
    SESSION_USER_SIGN_ENABLED.defaultVal.get)

  protected def eventEnabled: Boolean = true

  if (eventEnabled) EventBus.post(SparkOperationEvent(this))

  override protected def setState(newState: OperationState): Unit = {
    super.setState(newState)
    if (eventEnabled) {
      EventBus.post(SparkOperationEvent(
        this,
        operationListener.flatMap(_.getExecutionId),
        operationListener.map(_.getOperationRunTime),
        operationListener.map(_.getOperationCpuTime)))
      if (OperationState.isTerminal(newState)) {
        operationListener.foreach(l => {
          info(s"statementId=${statementId}, " +
            s"operationRunTime=${formatDuration(l.getOperationRunTime)}, " +
            s"operationCpuTime=${formatDuration(l.getOperationCpuTime / 1000000)}")
          session.asInstanceOf[SparkSessionImpl].increaseRunAndCpuTime(
            l.getOperationRunTime,
            l.getOperationCpuTime)
        })
      }
    }
  }

  protected def setSparkLocalProperty: (String, String) => Unit =
    spark.sparkContext.setLocalProperty

  protected def withLocalProperties[T](f: => T): T = {
    SQLExecution.withSQLConfPropagated(spark) {
      val originalSession = SparkSession.getActiveSession
      try {
        SparkSession.setActiveSession(spark)
        spark.sparkContext.setJobGroup(statementId, redactedStatement, forceCancel)
        spark.sparkContext.setLocalProperty(KYUUBI_SESSION_USER_KEY, session.user)
        spark.sparkContext.setLocalProperty(KYUUBI_STATEMENT_ID_KEY, statementId)
        schedulerPool match {
          case Some(pool) =>
            spark.sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL_KEY, pool)
          case None =>
        }
        if (isSessionUserSignEnabled) {
          setSessionUserSign()
        }

        f
      } finally {
        spark.sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL_KEY, null)
        spark.sparkContext.setLocalProperty(KYUUBI_SESSION_USER_KEY, null)
        spark.sparkContext.setLocalProperty(KYUUBI_STATEMENT_ID_KEY, null)
        spark.sparkContext.clearJobGroup()
        if (isSessionUserSignEnabled) {
          clearSessionUserSign()
        }
        originalSession match {
          case Some(session) => SparkSession.setActiveSession(session)
          case None => SparkSession.clearActiveSession()
        }
      }
    }
  }

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    // We should use Throwable instead of Exception since `java.lang.NoClassDefFoundError`
    // could be thrown.
    case e: Throwable =>
      // Prior SPARK-43952 (3.5.0), broadcast jobs uses a different group id, so we can not
      // cancel those broadcast jobs. See more details in SPARK-20774 (3.0.0)
      if (cancel && !spark.sparkContext.isStopped) spark.sparkContext.cancelJobGroup(statementId)
      withLockRequired {
        val errMsg = Utils.stringifyException(e)
        if (state == OperationState.TIMEOUT) {
          val ke = KyuubiSQLException(s"Timeout operating $opType: $errMsg")
          setOperationException(ke)
          throw ke
        } else if (isTerminalState(state)) {
          val ke = KyuubiSQLException(errMsg)
          setOperationException(ke)
          throw ke
        } else {
          error(s"Error operating $opType: $errMsg", e)
          val ke = KyuubiSQLException(s"Error operating $opType: $errMsg", e)
          setOperationException(ke)
          setState(OperationState.ERROR)
          throw ke
        }
      }
  }

  override protected def beforeRun(): Unit = {
    Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
    setHasResultSet(true)
    setState(OperationState.RUNNING)
  }

  override protected def afterRun(): Unit = {
    withLockRequired {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
    OperationLog.removeCurrentOperationLog()
  }

  override def cancel(): Unit = {
    cleanup(OperationState.CANCELED)
  }

  override def close(): Unit = {
    cleanup(OperationState.CLOSED)
    try {
      getOperationLog.foreach(_.close())
    } catch {
      case e: IOException =>
        error(e.getMessage, e)
    }
  }

  def getResultSetMetadataHints(): Seq[String] = Seq.empty

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val resp = new TGetResultSetMetadataResp
    val schema = SchemaHelper.toTTableSchema(resultSchema, timeZone.toString)
    resp.setSchema(schema)
    resp.setStatus(okStatusWithHints(getResultSetMetadataHints()))
    resp
  }

  override def getNextRowSetInternal(
      order: FetchOrientation,
      rowSetSize: Int): TFetchResultsResp = {
    var resultRowSet: TRowSet = null
    try {
      withLocalProperties {
        validateDefaultFetchOrientation(order)
        assertState(OperationState.FINISHED)
        setHasResultSet(true)
        order match {
          case FETCH_NEXT => iter.fetchNext()
          case FETCH_PRIOR => iter.fetchPrior(rowSetSize);
          case FETCH_FIRST => iter.fetchAbsolute(0);
        }
        resultRowSet =
          if (isArrowBasedOperation) {
            if (iter.hasNext) {
              val taken = iter.next().asInstanceOf[Array[Byte]]
              new SparkArrowTRowSetGenerator().toTRowSet(
                Seq(taken),
                new StructType().add(StructField(null, BinaryType)),
                getProtocolVersion)
            } else {
              ThriftUtils.newEmptyRowSet
            }
          } else {
            val taken = iter.take(rowSetSize)
            new SparkTRowSetGenerator().toTRowSet(
              taken.toSeq.asInstanceOf[Seq[Row]],
              resultSchema,
              getProtocolVersion)
          }
        resultRowSet.setStartRowOffset(iter.getPosition)
      }
    } catch onError(cancel = true)

    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(resultRowSet)
    resp.setHasMoreRows(false)
    resp
  }

  override def shouldRunAsync: Boolean = false

  protected def isArrowBasedOperation: Boolean = false

  protected def resultFormat: String = "thrift"

  protected def timestampAsString: Boolean = {
    spark.conf.get(ARROW_BASED_ROWSET_TIMESTAMP_AS_STRING.key, "false").toBoolean
  }

  protected def setSessionUserSign(): Unit = {
    (
      session.conf.get(KYUUBI_SESSION_SIGN_PUBLICKEY),
      session.conf.get(KYUUBI_SESSION_USER_SIGN)) match {
      case (Some(pubKey), Some(userSign)) =>
        setSparkLocalProperty(KYUUBI_SESSION_SIGN_PUBLICKEY, pubKey)
        setSparkLocalProperty(KYUUBI_SESSION_USER_SIGN, userSign)
      case _ =>
        throw new IllegalArgumentException(
          s"missing $KYUUBI_SESSION_SIGN_PUBLICKEY or $KYUUBI_SESSION_USER_SIGN" +
            s" in session config for session user sign")
    }
  }

  protected def clearSessionUserSign(): Unit = {
    setSparkLocalProperty(KYUUBI_SESSION_SIGN_PUBLICKEY, null)
    setSparkLocalProperty(KYUUBI_SESSION_USER_SIGN, null)
  }
}

object SparkOperation {
  val TIMEZONE_KEY = "spark.sql.session.timeZone"
}
