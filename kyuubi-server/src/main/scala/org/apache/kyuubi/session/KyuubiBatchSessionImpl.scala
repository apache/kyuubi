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

package org.apache.kyuubi.session

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.client.api.v1.dto.BatchRequest
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.events.{EventBus, KyuubiSessionEvent}
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.SessionType.SessionType

class KyuubiBatchSessionImpl(
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    override val sessionManager: KyuubiSessionManager,
    val sessionConf: KyuubiConf,
    batchRequest: BatchRequest,
    recoveryMetadata: Option[Metadata] = None)
  extends KyuubiSession(
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
    user,
    password,
    ipAddress,
    conf,
    sessionManager) {
  override val sessionType: SessionType = SessionType.BATCH

  override val handle: SessionHandle = recoveryMetadata.map { metadata =>
    SessionHandle(UUID.fromString(metadata.identifier))
  }.getOrElse(SessionHandle())

  override def createTime: Long = recoveryMetadata.map(_.createTime).getOrElse(super.createTime)

  override def getNoOperationTime: Long = {
    if (batchJobSubmissionOp != null && !OperationState.isTerminal(
        batchJobSubmissionOp.getStatus.state)) {
      0L
    } else {
      super.getNoOperationTime
    }
  }

  override val sessionIdleTimeoutThreshold: Long =
    sessionManager.getConf.get(KyuubiConf.BATCH_SESSION_IDLE_TIMEOUT)

  // TODO: Support batch conf advisor
  override val normalizedConf: Map[String, String] = {
    sessionConf.getBatchConf(batchRequest.getBatchType) ++
      sessionManager.validateBatchConf(batchRequest.getConf.asScala.toMap)
  }

  override lazy val name: Option[String] = Option(batchRequest.getName).orElse(
    normalizedConf.get(KyuubiConf.SESSION_NAME.key))

  // whether the resource file is from uploading
  private[kyuubi] val isResourceUploaded: Boolean = batchRequest.getConf
    .getOrDefault(KyuubiReservedKeys.KYUUBI_SESSION_BATCH_RESOURCE_UPLOADED_KEY, "false").toBoolean

  private[kyuubi] lazy val batchJobSubmissionOp = sessionManager.operationManager
    .newBatchJobSubmissionOperation(
      this,
      batchRequest.getBatchType,
      name.orNull,
      batchRequest.getResource,
      batchRequest.getClassName,
      normalizedConf,
      batchRequest.getArgs.asScala,
      recoveryMetadata)

  private def waitMetadataRequestsRetryCompletion(): Unit = {
    val batchId = batchJobSubmissionOp.batchId
    sessionManager.getMetadataRequestsRetryRef(batchId).foreach {
      metadataRequestsRetryRef =>
        while (metadataRequestsRetryRef.hasRemainingRequests()) {
          info(s"There are still remaining metadata store requests for batch[$batchId]")
          Thread.sleep(300)
        }
        sessionManager.deRegisterMetadataRequestsRetryRef(batchId)
    }
  }

  private val sessionEvent = KyuubiSessionEvent(this)
  recoveryMetadata.map(metadata => sessionEvent.engineId = metadata.engineId)
  EventBus.post(sessionEvent)

  override def getSessionEvent: Option[KyuubiSessionEvent] = {
    Option(sessionEvent)
  }

  override def checkSessionAccessPathURIs(): Unit = {
    KyuubiApplicationManager.checkApplicationAccessPaths(
      batchRequest.getBatchType,
      normalizedConf,
      sessionManager.getConf)
    if (batchRequest.getResource != SparkProcessBuilder.INTERNAL_RESOURCE
      && !isResourceUploaded) {
      KyuubiApplicationManager.checkApplicationAccessPath(
        batchRequest.getResource,
        sessionManager.getConf)
    }
  }

  override def open(): Unit = handleSessionException {
    traceMetricsOnOpen()

    if (recoveryMetadata.isEmpty) {
      val metaData = Metadata(
        identifier = handle.identifier.toString,
        sessionType = sessionType,
        realUser = realUser,
        username = user,
        ipAddress = ipAddress,
        kyuubiInstance = connectionUrl,
        state = OperationState.PENDING.toString,
        resource = batchRequest.getResource,
        className = batchRequest.getClassName,
        requestName = name.orNull,
        requestConf = normalizedConf,
        requestArgs = batchRequest.getArgs.asScala,
        createTime = createTime,
        engineType = batchRequest.getBatchType,
        clusterManager = batchJobSubmissionOp.builder.clusterManager())

      sessionManager.insertMetadata(metaData)
    }

    checkSessionAccessPathURIs()

    // create the operation root directory before running batch job submission operation
    super.open()

    runOperation(batchJobSubmissionOp)
    sessionEvent.totalOperations += 1
  }

  private[kyuubi] def onEngineOpened(): Unit = {
    if (sessionEvent.openedTime <= 0) {
      sessionEvent.openedTime = batchJobSubmissionOp.appStartTime
      EventBus.post(sessionEvent)
    }
  }

  override def close(): Unit = {
    super.close()
    batchJobSubmissionOp.close()
    waitMetadataRequestsRetryCompletion()
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    traceMetricsOnClose()
  }
}
