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

import org.apache.hive.service.rpc.thrift.{TGetResultSetMetadataResp, TRowSet}
import org.apache.spark.kyuubi.SparkUtilsHelper.redact
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SESSION_USER_SIGN_ENABLED
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_SIGN_PUBLICKEY, KYUUBI_SESSION_USER_KEY, KYUUBI_SESSION_USER_SIGN, KYUUBI_STATEMENT_ID_KEY}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_SCHEDULER_POOL_KEY
import org.apache.kyuubi.engine.spark.operation.SparkOperation.TIMEZONE_KEY
import org.apache.kyuubi.engine.spark.schema.{RowSet, SchemaHelper}
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.operation.{AbstractOperation, FetchIterator, OperationState}
import org.apache.kyuubi.operation.FetchOrientation._
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

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

  protected def resultSchema: StructType

  override def redactedStatement: String =
    redact(spark.sessionState.conf.stringRedactionPattern, statement)

  override def cleanup(targetState: OperationState): Unit = state.synchronized {
    if (!isTerminalState(state)) {
      setState(targetState)
      Option(getBackgroundHandle).foreach(_.cancel(true))
      if (!spark.sparkContext.isStopped) spark.sparkContext.cancelJobGroup(statementId)
    }
  }

  protected val forceCancel =
    session.sessionManager.getConf.get(KyuubiConf.OPERATION_FORCE_CANCEL)

  protected val schedulerPool =
    spark.conf.getOption(KyuubiConf.OPERATION_SCHEDULER_POOL.key).orElse(
      session.sessionManager.getConf.get(KyuubiConf.OPERATION_SCHEDULER_POOL))

  protected val isSessionUserSignEnabled: Boolean = spark.sparkContext.getConf.getBoolean(
    s"spark.${SESSION_USER_SIGN_ENABLED.key}",
    SESSION_USER_SIGN_ENABLED.defaultVal.get)

  protected def setSparkLocalProperty: (String, String) => Unit =
    spark.sparkContext.setLocalProperty

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

  def getResultSetMetadataHints(): Seq[String] = Seq.empty

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val resp = new TGetResultSetMetadataResp
    val schema = SchemaHelper.toTTableSchema(resultSchema, timeZone.toString)
    resp.setSchema(schema)
    resp.setStatus(okStatusWithHints(getResultSetMetadataHints()))
    resp
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet =
    withLocalProperties {
      var resultRowSet: TRowSet = null
      try {
        validateDefaultFetchOrientation(order)
        assertState(OperationState.FINISHED)
        setHasResultSet(true)
        order match {
          case FETCH_NEXT => iter.fetchNext()
          case FETCH_PRIOR => iter.fetchPrior(rowSetSize);
          case FETCH_FIRST => iter.fetchAbsolute(0);
        }
        resultRowSet =
          if (arrowEnabled) {
            if (iter.hasNext) {
              val taken = iter.next().asInstanceOf[Array[Byte]]
              RowSet.toTRowSet(taken, getProtocolVersion)
            } else {
              RowSet.emptyTRowSet()
            }
          } else {
            val taken = iter.take(rowSetSize)
            RowSet.toTRowSet(
              taken.toList.asInstanceOf[List[Row]],
              resultSchema,
              getProtocolVersion,
              timeZone)
          }
        resultRowSet.setStartRowOffset(iter.getPosition)
      } catch onError(cancel = true)

      resultRowSet
    }

  override def shouldRunAsync: Boolean = false

  protected def arrowEnabled(): Boolean = {
    resultCodec().equalsIgnoreCase("arrow") &&
    // TODO: (fchen) make all operation support arrow
    getClass.getCanonicalName == classOf[ExecuteStatement].getCanonicalName
  }

  protected def resultCodec(): String = {
    // TODO: respect the config of the operation ExecuteStatement, if it was set.
    spark.conf.get("kyuubi.operation.result.codec", "simple")
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
