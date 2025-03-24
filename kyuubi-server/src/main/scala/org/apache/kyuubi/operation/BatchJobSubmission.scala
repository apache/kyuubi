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

import java.nio.file.{Files, Paths}
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationState, KillResponse, ProcBuilder}
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.metrics.MetricsConstants.OPERATION_OPEN
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.{isTerminal, CANCELED, OperationState, RUNNING}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiBatchSession
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

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
    session: KyuubiBatchSession,
    val batchType: String,
    val batchName: String,
    resource: String,
    className: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    metadata: Option[Metadata])
  extends KyuubiApplicationOperation(session) {
  import BatchJobSubmission._

  override def shouldRunAsync: Boolean = true

  private val _operationLog = OperationLog.createOperationLog(session, getHandle)

  private val applicationManager = session.sessionManager.applicationManager

  private[kyuubi] val batchId: String = session.handle.identifier.toString

  @volatile private var _applicationInfo: Option[ApplicationInfo] = None
  def getApplicationInfo: Option[ApplicationInfo] = _applicationInfo

  private var killMessage: KillResponse = (false, "UNKNOWN")
  def getKillMessage: KillResponse = killMessage

  @volatile private var _appStartTime = metadata.map(_.engineOpenTime).getOrElse(0L)
  def appStartTime: Long = _appStartTime
  def appStarted: Boolean = _appStartTime > 0

  private lazy val _submitTime = if (appStarted) _appStartTime else System.currentTimeMillis

  @VisibleForTesting
  private[kyuubi] val builder: ProcBuilder = {
    val mainClass = Option(batchType).map(_.toUpperCase(Locale.ROOT)) match {
      case Some("SPARK") => className
      case Some("PYSPARK") => null
      case _ => throw new UnsupportedOperationException(s"Batch type $batchType unsupported")
    }
    new SparkBatchProcessBuilder(
      session.user,
      session.sessionConf,
      batchId,
      batchName,
      Option(resource),
      mainClass,
      batchConf,
      batchArgs,
      getOperationLog)
  }

  def startupProcessAlive: Boolean =
    builder.processLaunched && Option(builder.process).exists(_.isAlive)

  override def currentApplicationInfo(): Option[ApplicationInfo] = {
    if (isTerminal(state) && _applicationInfo.map(_.state).exists(ApplicationState.isTerminated)) {
      return _applicationInfo
    }
    val applicationInfo =
      applicationManager.getApplicationInfo(
        builder.appMgrInfo(),
        batchId,
        Some(session.user),
        Some(_submitTime))
    applicationId(applicationInfo).foreach { _ =>
      if (_appStartTime <= 0) {
        _appStartTime = System.currentTimeMillis()
      }
    }
    applicationInfo
  }

  private def applicationId(applicationInfo: Option[ApplicationInfo]): Option[String] = {
    applicationInfo.filter(_.id != null).map(_.id).orElse(None)
  }

  private[kyuubi] def killBatchApplication(): KillResponse = {
    applicationManager.killApplication(builder.appMgrInfo(), batchId, Some(session.user))
  }

  private val applicationCheckInterval =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_CHECK_INTERVAL)
  private val applicationStarvationTimeout =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_STARVATION_TIMEOUT)

  private val applicationStartupDestroyTimeout =
    session.sessionConf.get(KyuubiConf.SESSION_ENGINE_STARTUP_DESTROY_TIMEOUT)

  private def updateBatchMetadata(): Unit = {
    val endTime = if (isTerminalState(state)) lastAccessTime else 0L

    if (isTerminalState(state) && _applicationInfo.isEmpty) {
      _applicationInfo = Some(ApplicationInfo.NOT_FOUND)
    }

    _applicationInfo.foreach { appInfo =>
      val metadataToUpdate = Metadata(
        identifier = batchId,
        state = state.toString,
        engineOpenTime = appStartTime,
        engineId = appInfo.id,
        engineName = appInfo.name,
        engineUrl = appInfo.url.orNull,
        engineState = getAppState(state, appInfo.state).toString,
        engineError = appInfo.error,
        endTime = endTime)
      session.sessionManager.updateMetadata(metadataToUpdate)
    }
  }

  private def getAppState(
      opState: OperationState,
      appState: ApplicationState.ApplicationState): ApplicationState.ApplicationState = {
    if (opState == OperationState.ERROR && !ApplicationState.isTerminated(appState)) {
      withOperationLog(error(s"Batch $batchId state is $opState," +
        s" but the application state is $appState and not terminated, set to UNKNOWN."))
      ApplicationState.UNKNOWN
    } else {
      appState
    }
  }

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  // we can not set to other state if it is canceled
  private def setStateIfNotCanceled(newState: OperationState): Unit = withLockRequired {
    if (state != CANCELED) {
      setState(newState)
      applicationId(_applicationInfo).foreach { appId =>
        session.getSessionEvent.foreach(_.engineId = appId)
      }
      if (newState == RUNNING) {
        session.onEngineOpened()
      }
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

  override protected def runInternal(): Unit = {
    val asyncOperation: Runnable = () => {
      try {
        metadata match {
          case Some(metadata) if metadata.peerInstanceClosed =>
            setState(OperationState.CANCELED)
          case Some(metadata) if metadata.state == OperationState.PENDING.toString =>
            // case 1: new batch job created using batch impl v2
            // case 2: batch job from recovery, do submission only when previous state is
            // PENDING and fail to fetch the status by appId from resource manager, which
            // is similar with case 1; otherwise, monitor the submitted batch application.
            _applicationInfo = currentApplicationInfo()
            applicationId(_applicationInfo) match {
              case None => submitAndMonitorBatchJob()
              case Some(appId) => monitorBatchJob(appId)
            }
          case Some(metadata) =>
            // batch job from recovery which was submitted
            monitorBatchJob(metadata.engineId)
          case None =>
            // brand-new job created using batch impl v1
            submitAndMonitorBatchJob()
        }
        setStateIfNotCanceled(OperationState.FINISHED)
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

    def doUpdateApplicationInfoMetadataIfNeeded(): Unit = {
      updateApplicationInfoMetadataIfNeeded()
      if (!appStatusFirstUpdated) {
        // only the ApplicationInfo with non-empty id indicates that batch is RUNNING
        if (applicationId(_applicationInfo).isDefined) {
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
    }

    try {
      info(s"Submitting $batchType batch[$batchId] job:\n$builder")
      val process = builder.start
      while (process.isAlive && !applicationFailed(_applicationInfo)) {
        doUpdateApplicationInfoMetadataIfNeeded()
        process.waitFor(applicationCheckInterval, TimeUnit.MILLISECONDS)
      }

      if (!process.isAlive) {
        doUpdateApplicationInfoMetadataIfNeeded()
      }

      if (applicationFailed(_applicationInfo)) {
        Utils.terminateProcess(process, applicationStartupDestroyTimeout)
        throw new KyuubiException(s"Batch job failed: ${_applicationInfo}")
      }

      if (process.waitFor() != 0) {
        throw new KyuubiException(s"Process exit with value ${process.exitValue}")
      }

      while (!appStarted && applicationId(_applicationInfo).isEmpty &&
        !applicationTerminated(_applicationInfo)) {
        Thread.sleep(applicationCheckInterval)
        doUpdateApplicationInfoMetadataIfNeeded()
      }

      applicationId(_applicationInfo) match {
        case Some(appId) => monitorBatchJob(appId)
        case None if !appStarted =>
          throw new KyuubiException(s"$batchType batch[$batchId] job failed: ${_applicationInfo}")
        case None =>
      }
    } finally {
      val waitCompletion = batchConf.get(KyuubiConf.SESSION_ENGINE_STARTUP_WAIT_COMPLETION.key)
        .map(_.toBoolean).getOrElse(
          session.sessionConf.get(KyuubiConf.SESSION_ENGINE_STARTUP_WAIT_COMPLETION))
      val destroyProcess = !waitCompletion && builder.isClusterMode()
      if (destroyProcess) {
        info("Destroy the builder process because waitCompletion is false" +
          " and the engine is running in cluster mode.")
      }
      builder.close(destroyProcess)
      updateApplicationInfoMetadataIfNeeded()
      cleanupUploadedResourceIfNeeded()
    }
  }

  private def monitorBatchJob(appId: String): Unit = {
    info(s"Monitoring submitted $batchType batch[$batchId] job: $appId")
    if (_applicationInfo.isEmpty) {
      _applicationInfo = currentApplicationInfo()
    }
    if (state == OperationState.PENDING) {
      setStateIfNotCanceled(OperationState.RUNNING)
    }
    if (_applicationInfo.isEmpty) {
      info(s"The $batchType batch[$batchId] job: $appId not found, assume that it has finished.")
      return
    }
    if (applicationFailed(_applicationInfo)) {
      throw new KyuubiException(s"$batchType batch[$batchId] job failed: ${_applicationInfo}")
    }
    updateBatchMetadata()
    // TODO: add limit for max batch job submission lifetime
    while (_applicationInfo.isDefined && !applicationTerminated(_applicationInfo)) {
      Thread.sleep(applicationCheckInterval)
      updateApplicationInfoMetadataIfNeeded()
    }
    if (applicationFailed(_applicationInfo)) {
      throw new KyuubiException(s"$batchType batch[$batchId] job failed: ${_applicationInfo}")
    }
  }

  private def updateApplicationInfoMetadataIfNeeded(): Unit = {
    if (applicationId(_applicationInfo).isEmpty ||
      !_applicationInfo.map(_.state).exists(ApplicationState.isTerminated)) {
      val newApplicationStatus = currentApplicationInfo()
      if (newApplicationStatus.map(_.state) != _applicationInfo.map(_.state)) {
        _applicationInfo = newApplicationStatus
        updateBatchMetadata()
        info(s"Batch report for $batchId, ${_applicationInfo}")
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

  override def close(): Unit = withLockRequired(withClosingOperationLog {
    if (!isClosedOrCanceled) {
      MetricsSystem.tracing(_.decCount(MetricRegistry.name(OPERATION_OPEN, opType)))

      // fast fail
      if (isTerminalState(state)) {
        killMessage = (false, s"batch $batchId is already terminal so can not kill it.")
        builder.close(true)
        cleanupUploadedResourceIfNeeded()
        return
      }

      try {
        killMessage = killBatchApplication()
        builder.close(true)
        cleanupUploadedResourceIfNeeded()
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
            _applicationInfo = currentApplicationInfo()
            _applicationInfo.map(_.state) match {
              case Some(ApplicationState.FINISHED) =>
                setState(OperationState.FINISHED)
                updateBatchMetadata()
              case Some(ApplicationState.FAILED) =>
                setState(OperationState.ERROR)
                updateBatchMetadata()
              case Some(ApplicationState.UNKNOWN) |
                  Some(ApplicationState.NOT_FOUND) |
                  Some(ApplicationState.KILLED) =>
                setState(OperationState.CANCELED)
                updateBatchMetadata()
              case _ => // failed to kill, the kill message is enough
            }
          }
        }
      }
    }
  })

  override def cancel(): Unit = {
    throw new IllegalStateException("Use close instead.")
  }

  override def isTimedOut: Boolean = false

  override protected def eventEnabled: Boolean = true

  private def cleanupUploadedResourceIfNeeded(): Unit = {
    if (session.isResourceUploaded) {
      try {
        Files.deleteIfExists(Paths.get(resource))
      } catch {
        case e: Throwable => error(s"Error deleting the uploaded resource: $resource", e)
      }
    }
  }
}

object BatchJobSubmission {
  def applicationFailed(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isFailed)
  }

  def applicationTerminated(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isTerminated)
  }
}
