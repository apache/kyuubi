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

import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationOperation._
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.statestore.api._
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.ThreadUtils

class SessionStateStore extends AbstractService("SessionStateStore") {
  private var _stateStore: StateStore = _

  private lazy val restInstance = KyuubiRestFrontendService.getConnectionUrl
  private val stateStoreCleaner =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("session-state-store-cleaner")

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    _stateStore = StateStoreProvider.createStateStore(conf)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    startStateStoreCleaner()
  }

  override def stop(): Unit = {
    super.stop()
    ThreadUtils.shutdown(stateStoreCleaner)
    _stateStore.shutdown()
  }

  def createBatch(
      batchId: String,
      batchOwner: String,
      ipAddress: String,
      sessionConf: Map[String, String],
      batchRequest: BatchRequest): Unit = {
    val batchMetadata = BatchMetadata(
      batchId,
      batchOwner,
      ipAddress,
      sessionConf,
      restInstance,
      batchRequest.getBatchType,
      batchRequest.getResource,
      batchRequest.getClassName,
      batchRequest.getName,
      batchRequest.getConf.asScala.toMap,
      batchRequest.getArgs.asScala,
      OperationState.PENDING.toString,
      System.currentTimeMillis())
    _stateStore.createBatch(batchMetadata)
  }

  def getBatch(batchId: String): Batch = {
    Option(_stateStore.getBatch(batchId)).map(buildBatch).orNull
  }

  def updateBatchAppInfo(batchId: String, applicationStatus: Option[Map[String, String]]): Unit = {
    applicationStatus.foreach { appStatus =>
      _stateStore.updateBatchAppInfo(
        batchId,
        appStatus.get(APP_ID_KEY).orNull,
        appStatus.get(APP_NAME_KEY).orNull,
        appStatus.get(APP_URL_KEY).orNull,
        appStatus.get(APP_STATE_KEY).orNull,
        appStatus.get(APP_ERROR_KEY))
    }
  }

  def closeBatch(
      batchId: String,
      state: String,
      endTime: Long,
      finalAppStatus: Map[String, String]): Unit = {
    _stateStore.closeBatch(
      batchId,
      state,
      endTime,
      finalAppStatus.get(APP_ID_KEY).orNull,
      finalAppStatus.get(APP_NAME_KEY).orNull,
      finalAppStatus.get(APP_URL_KEY).orNull,
      finalAppStatus.get(APP_STATE_KEY).orNull,
      finalAppStatus.get(APP_ERROR_KEY))
  }

  def cleanupBatch(batchId: String): Unit = {
    _stateStore.cleanupBatch(batchId)
  }

  def getBatchesByType(batchType: String, from: Int, size: Int): Seq[Batch] = {
    _stateStore.getBatches(batchType, null, null, from, size).map(buildBatch)
  }

  private def buildBatch(batchMetadata: BatchMetadata): Batch = {
    val batchAppInfo = Map(
      APP_ID_KEY -> Option(batchMetadata.appId),
      APP_NAME_KEY -> Option(batchMetadata.appName),
      APP_STATE_KEY -> Option(batchMetadata.appState),
      APP_URL_KEY -> Option(batchMetadata.appUrl),
      APP_ERROR_KEY -> batchMetadata.appError)
      .filter(_._2.isDefined)
      .map(info => (info._1, info._2.get))

    new Batch(
      batchMetadata.batchId,
      batchMetadata.batchType,
      batchAppInfo.asJava,
      batchMetadata.kyuubiInstance,
      batchMetadata.state)
  }

  private def startStateStoreCleaner(): Unit = {
    val cleanerEnabled = conf.get(KyuubiConf.SERVER_STATE_STORE_CLEANER_ENABLED)

    if (cleanerEnabled) {
      val interval = conf.get(KyuubiConf.SERVER_STATE_STORE_CLEANER_INTERVAL)
      val cleanerTask: Runnable = () => {
        _stateStore.checkAndCleanupBatches()
      }

      stateStoreCleaner.scheduleWithFixedDelay(
        cleanerTask,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }
}
