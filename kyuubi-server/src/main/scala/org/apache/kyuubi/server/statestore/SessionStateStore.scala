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

package org.apache.kyuubi.server.statestore

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SERVER_STATE_STORE_MAX_AGE
import org.apache.kyuubi.engine.ApplicationOperation._
import org.apache.kyuubi.server.statestore.api.SessionMetadata
import org.apache.kyuubi.service.CompositeService
import org.apache.kyuubi.session.SessionType
import org.apache.kyuubi.util.{ClassUtils, ThreadUtils}

class SessionStateStore extends CompositeService("SessionStateStore") {
  private var _stateStore: StateStore = _

  private val stateStoreCleaner =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("session-state-store-cleaner")

  @VisibleForTesting
  private[statestore] val requestsRetryManager = new StateStoreRequestRetryManager(this)

  override def initialize(conf: KyuubiConf): Unit = {
    _stateStore = SessionStateStore.createStateStore(conf)
    addService(requestsRetryManager)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    startStateStoreCleaner()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(stateStoreCleaner)
    _stateStore.close()
    super.stop()
  }

  def insertMetadata(metadata: SessionMetadata, retryOnError: Boolean = true): Unit = {
    try {
      _stateStore.insertMetadata(metadata)
    } catch {
      case e: Throwable if retryOnError =>
        error(s"Error inserting metadata for session ${metadata.identifier}", e)
        val ref = requestsRetryManager.getOrCreateStateStoreRequestsRetryRef(metadata.identifier)
        ref.addRetryingSessionStateRequest(InsertMetadata(metadata))
    }
  }

  def getBatch(batchId: String): Batch = {
    Option(getBatchSessionMetadata(batchId)).map(buildBatch).orNull
  }

  def getBatchSessionMetadata(batchId: String): SessionMetadata = {
    Option(_stateStore.getMetadata(batchId, true)).filter(_.sessionType == SessionType.BATCH).orNull
  }

  def getBatches(
      batchType: String,
      batchUser: String,
      batchState: String,
      createTime: Long,
      endTime: Long,
      from: Int,
      size: Int): Seq[Batch] = {
    _stateStore.getMetadataList(
      SessionType.BATCH,
      batchType,
      batchUser,
      batchState,
      null,
      createTime,
      endTime,
      from,
      size,
      true).map(buildBatch)
  }

  def getBatchesRecoveryMetadata(
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[SessionMetadata] = {
    _stateStore.getMetadataList(
      SessionType.BATCH,
      null,
      null,
      state,
      kyuubiInstance,
      0,
      0,
      from,
      size,
      false)
  }

  def updateMetadata(metadata: SessionMetadata, retryOnError: Boolean = true): Unit = {
    try {
      _stateStore.updateMetadata(metadata)
    } catch {
      case e: Throwable if retryOnError =>
        error(s"Error updating metadata for session ${metadata.identifier}", e)
        val ref = requestsRetryManager.getOrCreateStateStoreRequestsRetryRef(metadata.identifier)
        ref.addRetryingSessionStateRequest(UpdateMetadata(metadata))
    }
  }

  def cleanupMetadataById(batchId: String): Unit = {
    _stateStore.cleanupMetadataByIdentifier(batchId)
  }

  private def buildBatch(batchMetadata: SessionMetadata): Batch = {
    val batchAppInfo = Map(
      APP_ID_KEY -> Option(batchMetadata.engineId),
      APP_NAME_KEY -> Option(batchMetadata.engineName),
      APP_STATE_KEY -> Option(batchMetadata.engineState),
      APP_URL_KEY -> Option(batchMetadata.engineUrl),
      APP_ERROR_KEY -> batchMetadata.engineError)
      .filter(_._2.isDefined)
      .map(info => (info._1, info._2.get))

    new Batch(
      batchMetadata.identifier,
      batchMetadata.username,
      batchMetadata.engineType,
      batchMetadata.requestName,
      batchAppInfo.asJava,
      batchMetadata.kyuubiInstance,
      batchMetadata.state,
      batchMetadata.createTime,
      batchMetadata.endTime)
  }

  private def startStateStoreCleaner(): Unit = {
    val cleanerEnabled = conf.get(KyuubiConf.SERVER_STATE_STORE_CLEANER_ENABLED)
    val stateMaxAge = conf.get(SERVER_STATE_STORE_MAX_AGE)

    if (cleanerEnabled) {
      val interval = conf.get(KyuubiConf.SERVER_STATE_STORE_CLEANER_INTERVAL)
      val cleanerTask: Runnable = () => {
        try {
          _stateStore.cleanupMetadataByAge(stateMaxAge)
        } catch {
          case e: Throwable => error("Error cleaning up the metadata by age", e)
        }
      }

      stateStoreCleaner.scheduleWithFixedDelay(
        cleanerTask,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }

  def getRequestsRetryRef(identifier: String): StateStoreRequestsRetryRef = {
    requestsRetryManager.getStateStoreRequestsRetryRef(identifier)
  }

  def deRegisterRequestsRetryRef(identifier: String): Unit = {
    requestsRetryManager.removeStateStoreRequestsRetryRef(identifier)
  }
}

object SessionStateStore extends Logging {
  def createStateStore(conf: KyuubiConf): StateStore = {
    val className = conf.get(KyuubiConf.SERVER_STATE_STORE_CLASS)
    if (className.isEmpty) {
      throw new KyuubiException(
        s"${KyuubiConf.SERVER_STATE_STORE_CLASS.key} cannot be empty.")
    }
    ClassUtils.createInstance(className, classOf[StateStore], conf)
  }
}
