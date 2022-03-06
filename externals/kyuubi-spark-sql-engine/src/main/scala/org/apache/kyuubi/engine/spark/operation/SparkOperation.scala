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

import org.apache.hive.service.rpc.thrift.{TRowSet, TTableSchema}
import org.apache.spark.kyuubi.SparkUtilsHelper.redact
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_USER_KEY, KYUUBI_STATEMENT_ID_KEY}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_SCHEDULER_POOL_KEY
import org.apache.kyuubi.engine.spark.operation.SparkOperation.TIMEZONE_KEY
import org.apache.kyuubi.engine.spark.schema.{RowSet, SchemaHelper}
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.operation.{AbstractOperation, FetchIterator, OperationState}
import org.apache.kyuubi.operation.FetchOrientation._
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

abstract class SparkOperation(opType: OperationType, session: Session)
  extends AbstractOperation(opType, session) {

  protected val spark: SparkSession = session.asInstanceOf[SparkSessionImpl].spark

  private val timeZone: ZoneId = {
    spark.conf.getOption(TIMEZONE_KEY).map { timeZoneId =>
      ZoneId.of(timeZoneId.replaceFirst("(\\+|\\-)(\\d):", "$10$2:"), ZoneId.SHORT_IDS)
    }.getOrElse(ZoneId.systemDefault())
  }

  protected var iter: FetchIterator[Row] = _

  protected var result: DataFrame = _

  protected def resultSchema: StructType

  override def redactedStatement: String =
    redact(spark.sessionState.conf.stringRedactionPattern, statement)

  protected def cleanup(targetState: OperationState): Unit = state.synchronized {
    if (!isTerminalState(state)) {
      setState(targetState)
      Option(getBackgroundHandle).foreach(_.cancel(true))
      if (!spark.sparkContext.isStopped) spark.sparkContext.cancelJobGroup(statementId)
    }
  }

  private val forceCancel =
    session.sessionManager.getConf.get(KyuubiConf.OPERATION_FORCE_CANCEL)

  private val schedulerPool =
    spark.conf.getOption(KyuubiConf.OPERATION_SCHEDULER_POOL.key).orElse(
      session.sessionManager.getConf.get(KyuubiConf.OPERATION_SCHEDULER_POOL))

  protected def withLocalProperties[T](f: => T): T = {
    try {
      spark.sparkContext.setJobGroup(statementId, redactedStatement, forceCancel)
      spark.sparkContext.setLocalProperty(KYUUBI_SESSION_USER_KEY, session.user)
      spark.sparkContext.setLocalProperty(KYUUBI_STATEMENT_ID_KEY, statementId)
      schedulerPool match {
        case Some(pool) =>
          spark.sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL_KEY, pool)
        case None =>
      }

      f
    } finally {
      spark.sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL_KEY, null)
      spark.sparkContext.setLocalProperty(KYUUBI_SESSION_USER_KEY, null)
      spark.sparkContext.setLocalProperty(KYUUBI_STATEMENT_ID_KEY, null)
      spark.sparkContext.clearJobGroup()
    }
  }

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    // We should use Throwable instead of Exception since `java.lang.NoClassDefFoundError`
    // could be thrown.
    case e: Throwable =>
      if (cancel && !spark.sparkContext.isStopped) spark.sparkContext.cancelJobGroup(statementId)
      state.synchronized {
        val errMsg = Utils.stringifyException(e)
        if (state == OperationState.TIMEOUT) {
          val ke = KyuubiSQLException(s"Timeout operating $opType: $errMsg")
          setOperationException(ke)
          throw ke
        } else if (isTerminalState(state)) {
          setOperationException(KyuubiSQLException(errMsg))
          warn(s"Ignore exception in terminal state with $statementId: $errMsg")
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
    state.synchronized {
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

  override def getResultSetSchema: TTableSchema = SchemaHelper.toTTableSchema(resultSchema)

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    order match {
      case FETCH_NEXT => iter.fetchNext()
      case FETCH_PRIOR => iter.fetchPrior(rowSetSize);
      case FETCH_FIRST => iter.fetchAbsolute(0);
    }
    val taken = iter.take(rowSetSize)
    val resultRowSet = RowSet.toTRowSet(taken.toList, resultSchema, getProtocolVersion, timeZone)
    resultRowSet.setStartRowOffset(iter.getPosition)
    resultRowSet
  }

  override def shouldRunAsync: Boolean = false
}

object SparkOperation {
  val TIMEZONE_KEY = "spark.sql.session.timeZone"
}
