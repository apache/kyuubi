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

package org.apache.kyuubi.server.metadata

import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.METADATA_MAX_AGE
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.SessionType
import org.apache.kyuubi.util.{ClassUtils, ThreadUtils}

class MetadataManager extends AbstractService("MetadataManager") {
  import MetadataManager._

  private var _metadataStore: MetadataStore = _

  // Visible for testing.
  private[metadata] val identifierRequestsRetryRefs =
    new ConcurrentHashMap[String, MetadataRequestsRetryRef]()

  // Visible for testing.
  private[metadata] val identifierRequestsRetryingCounts =
    new ConcurrentHashMap[String, AtomicInteger]()

  private val requestsRetryTrigger =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("metadata-requests-retry-trigger")

  private var requestsRetryExecutor: ThreadPoolExecutor = _

  private var maxMetadataRequestsRetryRefs: Int = _

  private val metadataCleaner =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("metadata-cleaner")

  override def initialize(conf: KyuubiConf): Unit = {
    _metadataStore = MetadataManager.createMetadataStore(conf)
    val retryExecutorNumThreads =
      conf.get(KyuubiConf.METADATA_REQUEST_RETRY_THREADS)
    requestsRetryExecutor = ThreadUtils.newDaemonFixedThreadPool(
      retryExecutorNumThreads,
      "metadata-requests-retry-executor")
    maxMetadataRequestsRetryRefs = conf.get(KyuubiConf.METADATA_REQUEST_RETRY_QUEUE_SIZE)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    startMetadataRequestsRetryTrigger()
    startMetadataCleaner()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(requestsRetryTrigger)
    ThreadUtils.shutdown(requestsRetryExecutor)
    ThreadUtils.shutdown(metadataCleaner)
    _metadataStore.close()
    super.stop()
  }

  private def withMetadataRequestMetrics[T](block: => T): T = {
    MetricsSystem.tracing(_.incCount(MetricsConstants.METADATA_REQUEST_OPENED))
    try {
      block
    } catch {
      case e: Throwable =>
        MetricsSystem.tracing(_.markMeter(MetricsConstants.METADATA_REQUEST_FAIL))
        throw e
    } finally {
      MetricsSystem.tracing(_.decCount(MetricsConstants.METADATA_REQUEST_OPENED))
      MetricsSystem.tracing(_.markMeter(MetricsConstants.METADATA_REQUEST_TOTAL))
    }
  }

  def insertMetadata(metadata: Metadata, retryOnError: Boolean = true): Unit = {
    try {
      withMetadataRequestMetrics(_metadataStore.insertMetadata(metadata))
    } catch {
      case e: Throwable if retryOnError =>
        error(s"Error inserting metadata for session ${metadata.identifier}", e)
        addMetadataRetryRequest(InsertMetadata(metadata))
    }
  }

  def getBatch(batchId: String): Batch = {
    Option(getBatchSessionMetadata(batchId)).map(buildBatch).orNull
  }

  def getBatchSessionMetadata(batchId: String): Metadata = {
    Option(withMetadataRequestMetrics(_metadataStore.getMetadata(batchId, true))).filter(
      _.sessionType == SessionType.BATCH).orNull
  }

  def getBatches(
      batchType: String,
      batchUser: String,
      batchState: String,
      createTime: Long,
      endTime: Long,
      from: Int,
      size: Int): Seq[Batch] = {
    val filter = MetadataFilter(
      sessionType = SessionType.BATCH,
      engineType = batchType,
      username = batchUser,
      state = batchState,
      createTime = createTime,
      endTime = endTime)
    withMetadataRequestMetrics(_metadataStore.getMetadataList(filter, from, size, true)).map(
      buildBatch)
  }

  def getBatchesRecoveryMetadata(
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[Metadata] = {
    val filter = MetadataFilter(
      sessionType = SessionType.BATCH,
      state = state,
      kyuubiInstance = kyuubiInstance)
    withMetadataRequestMetrics(_metadataStore.getMetadataList(filter, from, size, false))
  }

  def getPeerInstanceClosedBatchesMetadata(
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[Metadata] = {
    val filter = MetadataFilter(
      sessionType = SessionType.BATCH,
      state = state,
      kyuubiInstance = kyuubiInstance,
      peerInstanceClosed = true)
    withMetadataRequestMetrics(_metadataStore.getMetadataList(filter, from, size, true))
  }

  def updateMetadata(metadata: Metadata, retryOnError: Boolean = true): Unit = {
    try {
      withMetadataRequestMetrics(_metadataStore.updateMetadata(metadata))
    } catch {
      case e: Throwable if retryOnError =>
        error(s"Error updating metadata for session ${metadata.identifier}", e)
        addMetadataRetryRequest(UpdateMetadata(metadata))
    }
  }

  def cleanupMetadataById(batchId: String): Unit = {
    withMetadataRequestMetrics(_metadataStore.cleanupMetadataByIdentifier(batchId))
  }

  private def startMetadataCleaner(): Unit = {
    val cleanerEnabled = conf.get(KyuubiConf.METADATA_CLEANER_ENABLED)
    val stateMaxAge = conf.get(METADATA_MAX_AGE)

    if (cleanerEnabled) {
      val interval = conf.get(KyuubiConf.METADATA_CLEANER_INTERVAL)
      val cleanerTask: Runnable = () => {
        try {
          withMetadataRequestMetrics(_metadataStore.cleanupMetadataByAge(stateMaxAge))
        } catch {
          case e: Throwable => error("Error cleaning up the metadata by age", e)
        }
      }

      metadataCleaner.scheduleWithFixedDelay(
        cleanerTask,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }

  def addMetadataRetryRequest(request: MetadataRequest): Unit = {
    if (identifierRequestsRetryRefs.size() > maxMetadataRequestsRetryRefs) {
      throw new KyuubiException(
        "The number of metadata requests retry instances exceeds the limitation:" +
          maxMetadataRequestsRetryRefs)
    }
    val identifier = request.metadata.identifier
    val ref = identifierRequestsRetryRefs.computeIfAbsent(
      identifier,
      identifier => {
        val ref = new MetadataRequestsRetryRef
        debug(s"Created MetadataRequestsRetryRef for session $identifier.")
        ref
      })
    ref.addRetryingMetadataRequest(request)
    identifierRequestsRetryRefs.putIfAbsent(identifier, ref)
    MetricsSystem.tracing(_.markMeter(MetricsConstants.METADATA_REQUEST_RETRYING))
  }

  def getMetadataRequestsRetryRef(identifier: String): MetadataRequestsRetryRef = {
    identifierRequestsRetryRefs.get(identifier)
  }

  def deRegisterRequestsRetryRef(identifier: String): Unit = {
    identifierRequestsRetryRefs.remove(identifier)
    identifierRequestsRetryingCounts.remove(identifier)
  }

  private def startMetadataRequestsRetryTrigger(): Unit = {
    val interval = conf.get(KyuubiConf.METADATA_REQUEST_RETRY_INTERVAL)
    val triggerTask = new Runnable {
      override def run(): Unit = {
        identifierRequestsRetryRefs.forEach { (id, ref) =>
          if (!ref.hasRemainingRequests()) {
            identifierRequestsRetryRefs.remove(id)
            identifierRequestsRetryingCounts.remove(id)
          } else {
            val retryingCount =
              identifierRequestsRetryingCounts.computeIfAbsent(id, _ => new AtomicInteger(0))

            if (retryingCount.get() == 0) {
              val retryTask = new Runnable {
                override def run(): Unit = {
                  try {
                    info(s"Retrying metadata requests for $id")
                    var request = ref.metadataRequests.peek()
                    while (request != null) {
                      request match {
                        case insert: InsertMetadata =>
                          insertMetadata(insert.metadata, retryOnError = false)

                        case update: UpdateMetadata =>
                          updateMetadata(update.metadata, retryOnError = false)

                        case _ =>
                      }
                      ref.metadataRequests.remove(request)
                      MetricsSystem.tracing(_.markMeter(
                        MetricsConstants.METADATA_REQUEST_RETRYING,
                        -1L))
                      request = ref.metadataRequests.peek()
                    }
                  } catch {
                    case e: Throwable =>
                      error(s"Error retrying metadata requests for $id", e)
                  } finally {
                    retryingCount.decrementAndGet()
                  }
                }
              }

              try {
                retryingCount.incrementAndGet()
                requestsRetryExecutor.submit(retryTask)
              } catch {
                case e: Throwable =>
                  error(s"Error submitting metadata retry requests for $id", e)
                  retryingCount.decrementAndGet()
              }
            }

          }
        }
      }
    }
    requestsRetryTrigger.scheduleWithFixedDelay(
      triggerTask,
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }
}

object MetadataManager extends Logging {
  def createMetadataStore(conf: KyuubiConf): MetadataStore = {
    val className = conf.get(KyuubiConf.METADATA_STORE_CLASS)
    if (className.isEmpty) {
      throw new KyuubiException(
        s"${KyuubiConf.METADATA_STORE_CLASS.key} cannot be empty.")
    }
    ClassUtils.createInstance(className, classOf[MetadataStore], conf)
  }

  def buildBatch(batchMetadata: Metadata): Batch = {
    val batchState =
      if (batchMetadata.peerInstanceClosed &&
        !OperationState.isTerminal(OperationState.withName(batchMetadata.state))) {
        OperationState.CANCELED.toString
      } else {
        batchMetadata.state
      }

    val name = Option(batchMetadata.requestName).getOrElse(Option(batchMetadata.engineName).orNull)

    new Batch(
      batchMetadata.identifier,
      batchMetadata.username,
      batchMetadata.engineType,
      name,
      batchMetadata.engineId,
      batchMetadata.engineUrl,
      batchMetadata.engineState,
      batchMetadata.engineError.orNull,
      batchMetadata.kyuubiInstance,
      batchState,
      batchMetadata.createTime,
      batchMetadata.endTime)
  }
}
