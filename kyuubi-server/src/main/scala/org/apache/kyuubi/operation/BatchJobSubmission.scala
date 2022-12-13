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
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationState, KillResponse, ProcBuilder}
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.metrics.MetricsConstants.OPERATION_OPEN
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.{CANCELED, OperationState}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiBatchSessionImpl

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
  extends KyuubiApplicationOperation(session) {
  import BatchJobSubmission._

  override def shouldRunAsync: Boolean = true

  private val _operationLog = OperationLog.createOperationLog(session, getHandle)

  private val applicationManager = session.sessionManager.applicationManager

  private[kyuubi] val batchId: String = session.handle.identifier.toString

  private var applicationInfo: Option[ApplicationInfo] = None

  private var killMessage: KillResponse = (false, "UNKNOWN")
  def getKillMessage: KillResponse = killMessage

  @VisibleForTesting
  private[kyuubi] val builder: ProcBuilder = {
    Option(batchType).map(_.toUpperCase(Locale.ROOT)) match {
      case Some("SPARK") | Some("PYSPARK") =>
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

  override private[kyuubi] def currentApplicationInfo: Option[ApplicationInfo] = {
    // only the ApplicationInfo with non-empty id is valid for the operation
    applicationManager.getApplicationInfo(builder.clusterManager(), batchId).filter(_.id != null)
  }

  private[kyuubi] def killBatchApplication(): KillResponse = {
    applicationManager.killApplication(builder.clusterManager(), batchId)
  }

  private val applicationCheckInterval =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_CHECK_INTERVAL)
  private val applicationStarvationTimeout =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_STARVATION_TIMEOUT)

  private def updateBatchMetadata(): Unit = {
    val endTime =
      if (isTerminalState(state)) {
        lastAccessTime
      } else {
        0L
      }

    if (isTerminalState(state)) {
      if (applicationInfo.isEmpty) {
        applicationInfo =
          Option(ApplicationInfo(id = null, name = null, state = ApplicationState.NOT_FOUND))
      }
    }

    applicationInfo.foreach { status =>
      val metadataToUpdate = Metadata(
        identifier = batchId,
        state = state.toString,
        engineId = status.id,
        engineName = status.name,
        engineUrl = status.url.orNull,
        engineState = status.state.toString,
        engineError = status.error,
        endTime = endTime)
      session.sessionManager.updateMetadata(metadataToUpdate)
    }
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
      try {
        if (recoveryMetadata.exists(_.peerInstanceClosed)) {
          setState(OperationState.CANCELED)
        } else {
          // If it is in recovery mode, only re-submit batch job if previous state is PENDING and
          // fail to fetch the status including appId from resource manager. Otherwise, monitor the
          // submitted batch application.
          recoveryMetadata.map { metadata =>
            if (metadata.state == OperationState.PENDING.toString) {
              applicationInfo = currentApplicationInfo
              applicationInfo.map(_.id) match {
                case Some(null) =>
                  submitAndMonitorBatchJob()
                case Some(appId) =>
                  monitorBatchJob(appId)
                case None =>
                  submitAndMonitorBatchJob()
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

  private def submitAndMonitorBatchJob(): Unit = {
    var appStatusFirstUpdated = false
    var lastStarvationCheckTime = createTime
    try {
      info(s"Submitting $batchType batch[$batchId] job:\n$builder")
      val process = builder.start
      applicationInfo = currentApplicationInfo
      while (!applicationFailed(applicationInfo) && process.isAlive) {
        if (!appStatusFirstUpdated) {
          if (applicationInfo.isDefined) {
            setStateIfNotCanceled(OperationState.RUNNING)
            updateBatchMetadata()
            appStatusFirstUpdated = true
          } else {
            val currentTime = System.currentTimeMillis()
            if (currentTime - lastStarvationCheckTime > applicationStarvationTimeout) {
              lastStarvationCheckTime = currentTime
              warn(s"Batch[$batchId] has not started, check the Kyuubi server to ensure" +
                s" that batch jobs can be submitted.")
            }
          }
        }
        process.waitFor(applicationCheckInterval, TimeUnit.MILLISECONDS)
        applicationInfo = currentApplicationInfo
      }

      if (applicationFailed(applicationInfo)) {
        process.destroyForcibly()
        throw new RuntimeException(s"Batch job failed: $applicationInfo")
      } else {
        process.waitFor()
        if (process.exitValue() != 0) {
          throw new KyuubiException(s"Process exit with value ${process.exitValue()}")
        }

        Option(applicationInfo.map(_.id)).foreach {
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
    if (applicationInfo.isEmpty) {
      applicationInfo = currentApplicationInfo
    }
    if (state == OperationState.PENDING) {
      setStateIfNotCanceled(OperationState.RUNNING)
    }
    if (applicationInfo.isEmpty) {
      info(s"The $batchType batch[$batchId] job: $appId not found, assume that it has finished.")
    } else if (applicationFailed(applicationInfo)) {
      throw new RuntimeException(s"$batchType batch[$batchId] job failed: $applicationInfo")
    } else {
      updateBatchMetadata()
      // TODO: add limit for max batch job submission lifetime
      while (applicationInfo.isDefined && !applicationTerminated(applicationInfo)) {
        Thread.sleep(applicationCheckInterval)
        val newApplicationStatus = currentApplicationInfo
        if (newApplicationStatus.map(_.state) != applicationInfo.map(_.state)) {
          applicationInfo = newApplicationStatus
          info(s"Batch report for $batchId, $applicationInfo")
        }
      }

      if (applicationFailed(applicationInfo)) {
        throw new RuntimeException(s"$batchType batch[$batchId] job failed: $applicationInfo")
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

  override def close(): Unit = state.synchronized {
    if (!isClosedOrCanceled) {
      try {
        getOperationLog.foreach(_.close())
      } catch {
        case e: IOException => error(e.getMessage, e)
      }

      MetricsSystem.tracing(_.decCount(MetricRegistry.name(OPERATION_OPEN, opType)))

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
        if (state == OperationState.INITIALIZED) {
          // if state is INITIALIZED, it means that the batch submission has not started to run, set
          // the state to CANCELED manually and regardless of kill result
          setState(OperationState.CANCELED)
          updateBatchMetadata()
        } else {
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
  }

  override def cancel(): Unit = {
    throw new IllegalStateException("Use close instead.")
  }

  override def isTimedOut: Boolean = false
}

object BatchJobSubmission {
  def applicationFailed(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isFailed)
  }

  def applicationTerminated(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isTerminated)
  }
}
