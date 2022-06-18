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

package org.apache.kyuubi.operation

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{ArrayList => JArrayList, Locale}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.{ApplicationOperation, KillResponse, ProcBuilder}
import org.apache.kyuubi.engine.ApplicationOperation._
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.metrics.MetricsConstants.OPERATION_OPEN
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.{CANCELED, OperationState}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiBatchSessionImpl
import org.apache.kyuubi.util.ThriftUtils

/**
 * The state of batch operation is special. In general, the lifecycle of state is:
 *
 *                        /  ERROR
 * PENDING  ->  RUNNING  ->  FINISHED
 *                        \  CANCELED (CLOSED)
 *
 * We can not change FINISHED/ERROR/CANCELED to CLOSED, and it's different with other operation
 * which final status is always CLOSED, so we do not use CLOSED state in this class.
 * To compatible with kill application we combine the semantics of `cancel` and `close`, so if
 * user close the batch session that means the final status is CANCELED.
 */
class BatchJobSubmission(
    session: KyuubiBatchSessionImpl,
    val batchType: String,
    val batchName: String,
    resource: String,
    className: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    recoveryMetadata: Option[Metadata])
  extends KyuubiOperation(OperationType.UNKNOWN_OPERATION, session) {

  override def statement: String = "BATCH_JOB_SUBMISSION"

  override def shouldRunAsync: Boolean = true

  private val _operationLog = OperationLog.createOperationLog(session, getHandle)

  private val applicationManager = session.sessionManager.applicationManager

  private[kyuubi] val batchId: String = session.handle.identifier.toString

  private var applicationStatus: Option[Map[String, String]] = None

  private var killMessage: KillResponse = (false, "UNKNOWN")
  def getKillMessage: KillResponse = killMessage

  @VisibleForTesting
  private[kyuubi] val builder: ProcBuilder = {
    Option(batchType).map(_.toUpperCase(Locale.ROOT)) match {
      case Some("SPARK") =>
        new SparkBatchProcessBuilder(
          session.user,
          session.sessionConf,
          batchId,
          batchName,
          Option(resource),
          className,
          batchConf,
          batchArgs,
          getOperationLog)

      case _ =>
        throw new UnsupportedOperationException(s"Batch type $batchType unsupported")
    }
  }

  private[kyuubi] def currentApplicationState: Option[Map[String, String]] = {
    applicationManager.getApplicationInfo(builder.clusterManager(), batchId)
  }

  private[kyuubi] def killBatchApplication(): KillResponse = {
    applicationManager.killApplication(builder.clusterManager(), batchId)
  }

  private val applicationCheckInterval =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_CHECK_INTERVAL)

  private def updateBatchMetadata(): Unit = {
    val endTime =
      if (isTerminalState(state)) {
        lastAccessTime
      } else {
        0L
      }

    val engineAppStatus = applicationStatus.getOrElse(Map.empty)
    val metadataToUpdate = Metadata(
      identifier = batchId,
      state = state.toString,
      engineId = engineAppStatus.get(APP_ID_KEY).orNull,
      engineName = engineAppStatus.get(APP_NAME_KEY).orNull,
      engineUrl = engineAppStatus.get(APP_URL_KEY).orNull,
      engineState = engineAppStatus.get(APP_STATE_KEY).orNull,
      engineError = engineAppStatus.get(APP_ERROR_KEY),
      endTime = endTime)
    session.sessionManager.updateMetadata(metadataToUpdate)
  }

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  // we can not set to other state if it is canceled
  private def setStateIfNotCanceled(newState: OperationState): Unit = state.synchronized {
    if (state != CANCELED) {
      setState(newState)
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setStateIfNotCanceled(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = session.handleSessionException {
    val asyncOperation: Runnable = () => {
      setStateIfNotCanceled(OperationState.RUNNING)
      try {
        if (recoveryMetadata.exists(_.remoteClosed)) {
          setState(OperationState.CANCELED)
        } else {
          // If it is in recovery mode, only re-submit batch job if previous state is PENDING and
          // fail to fetch the status including appId from resource manager. Otherwise, monitor the
          // submitted batch application.
          recoveryMetadata.map { metadata =>
            if (metadata.state == OperationState.PENDING.toString) {
              applicationStatus = currentApplicationState
              applicationStatus.map(_.get(APP_ID_KEY)).map {
                case Some(appId) => monitorBatchJob(appId)
                case None => submitAndMonitorBatchJob()
              }
            } else {
              monitorBatchJob(metadata.engineId)
            }
          }.getOrElse {
            submitAndMonitorBatchJob()
          }
          setStateIfNotCanceled(OperationState.FINISHED)
        }
      } catch {
        onError()
      } finally {
        updateBatchMetadata()
      }
    }

    try {
      val opHandle = session.sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch {
      onError("submitting batch job submission operation in background, request rejected")
    } finally {
      if (isTerminalState(state)) {
        updateBatchMetadata()
      }
    }
  }

  private def applicationFailed(applicationStatus: Option[Map[String, String]]): Boolean = {
    applicationStatus.map(_.get(ApplicationOperation.APP_STATE_KEY)).exists(s =>
      s.contains("KILLED") || s.contains("FAILED"))
  }

  private def applicationTerminated(applicationStatus: Option[Map[String, String]]): Boolean = {
    applicationStatus.map(_.get(ApplicationOperation.APP_STATE_KEY)).exists(s =>
      s.contains("KILLED") || s.contains("FAILED") || s.contains("FINISHED"))
  }

  private def submitAndMonitorBatchJob(): Unit = {
    var appStatusFirstUpdated = false
    try {
      info(s"Submitting $batchType batch[$batchId] job: $builder")
      val process = builder.start
      applicationStatus = currentApplicationState
      while (!applicationFailed(applicationStatus) && process.isAlive) {
        if (!appStatusFirstUpdated && applicationStatus.isDefined) {
          updateBatchMetadata()
          appStatusFirstUpdated = true
        }
        process.waitFor(applicationCheckInterval, TimeUnit.MILLISECONDS)
        applicationStatus = currentApplicationState
      }

      if (applicationFailed(applicationStatus)) {
        process.destroyForcibly()
        throw new RuntimeException("Batch job failed:" + applicationStatus.get.mkString(","))
      } else {
        process.waitFor()
        if (process.exitValue() != 0) {
          throw new KyuubiException(s"Process exit with value ${process.exitValue()}")
        }

        applicationStatus.map(_.get(APP_ID_KEY)).map {
          case Some(appId) => monitorBatchJob(appId)
          case _ =>
        }
      }
    } finally {
      builder.close()
    }
  }

  private def monitorBatchJob(appId: String): Unit = {
    info(s"Monitoring submitted $batchType batch[$batchId] job: $appId")
    if (applicationStatus.isEmpty) {
      applicationStatus = currentApplicationState
    }
    if (applicationStatus.isEmpty) {
      info(s"The $batchType batch[$batchId] job: $appId not found, assume that it has finished.")
    } else if (applicationFailed(applicationStatus)) {
      throw new RuntimeException(s"$batchType batch[$batchId] job failed:" +
        applicationStatus.get.mkString(","))
    } else {
      // TODO: add limit for max batch job submission lifetime
      while (applicationStatus.isDefined && !applicationTerminated(applicationStatus)) {
        Thread.sleep(applicationCheckInterval)
        val newApplicationStatus = currentApplicationState
        if (newApplicationStatus != applicationStatus) {
          applicationStatus = newApplicationStatus
          info(s"Batch report for $batchId" +
            applicationStatus.map(_.mkString("(", ",", ")")).getOrElse("()"))
        }
      }

      if (applicationFailed(applicationStatus)) {
        throw new RuntimeException(s"$batchType batch[$batchId] job failed:" +
          applicationStatus.get.mkString(","))
      }
    }
  }

  def getOperationLogRowSet(
      order: FetchOrientation,
      from: Int,
      size: Int): TRowSet = {
    val operationLog = getOperationLog
    operationLog.map(_.read(from, size)).getOrElse {
      throw KyuubiSQLException(s"Batch ID: $batchId, failed to generate operation log")
    }
  }

  override val getResultSetSchema: TTableSchema = {
    val schema = new TTableSchema()
    Seq("key", "value").zipWithIndex.foreach { case (colName, position) =>
      val tColumnDesc = new TColumnDesc()
      tColumnDesc.setColumnName(colName)
      val tTypeDesc = new TTypeDesc()
      tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
      tColumnDesc.setTypeDesc(tTypeDesc)
      tColumnDesc.setPosition(position)
      schema.addToColumns(tColumnDesc)
    }
    schema
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    currentApplicationState.map { state =>
      val tRow = new TRowSet(0, new JArrayList[TRow](state.size))
      Seq(state.keys, state.values).map(_.toSeq.asJava).foreach { col =>
        val tCol = TColumn.stringVal(new TStringColumn(col, ByteBuffer.allocate(0)))
        tRow.addToColumns(tCol)
      }
      tRow
    }.getOrElse(ThriftUtils.EMPTY_ROW_SET)
  }

  override def close(): Unit = state.synchronized {
    if (!isClosedOrCanceled) {
      try {
        getOperationLog.foreach(_.close())
      } catch {
        case e: IOException => error(e.getMessage, e)
      }

      MetricsSystem.tracing(_.decCount(
        MetricRegistry.name(OPERATION_OPEN, statement.toLowerCase(Locale.getDefault))))

      // fast fail
      if (isTerminalState(state)) {
        killMessage = (false, s"batch $batchId is already terminal so can not kill it.")
        builder.close()
        return
      }

      try {
        killMessage = killBatchApplication()
        builder.close()
      } finally {
        if (killMessage._1 && !isTerminalState(state)) {
          // kill success and we can change state safely
          // note that, the batch operation state should never be closed
          setState(OperationState.CANCELED)
          updateBatchMetadata()
        } else if (killMessage._1) {
          // we can not change state safely
          killMessage = (false, s"batch $batchId is already terminal so can not kill it.")
        } else if (!isTerminalState(state)) {
          // failed to kill, the kill message is enough
        }
      }
    }
  }

  override def cancel(): Unit = {
    throw new IllegalStateException("Use close instead.")
  }
}
