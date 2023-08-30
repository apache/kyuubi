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

package org.apache.kyuubi.server

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kyuubi.config.KyuubiConf.BATCH_SUBMITTER_THREADS
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.service.{AbstractService, Serverable}
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.ThreadUtils

class KyuubiBatchService(
    server: Serverable,
    sessionManager: KyuubiSessionManager)
  extends AbstractService(classOf[KyuubiBatchService].getSimpleName) {

  private lazy val restFrontend = server.frontendServices
    .filter(_.isInstanceOf[KyuubiRestFrontendService])
    .head

  private def kyuubiInstance: String = restFrontend.connectionUrl

  // TODO expose metrics, including pending/running/succeeded/failed batches
  // TODO handle dangling batches, e.g. batch is picked and changed state to pending,
  //      but the Server crashed before submitting or updating status to metastore

  private lazy val metadataManager: MetadataManager = sessionManager.metadataManager.get
  private val running: AtomicBoolean = new AtomicBoolean(false)
  private lazy val batchExecutor = ThreadUtils
    .newDaemonFixedThreadPool(conf.get(BATCH_SUBMITTER_THREADS), "kyuubi-batch-submitter")

  def cancelUnscheduledBatch(batchId: String): Boolean = {
    metadataManager.cancelUnscheduledBatch(batchId)
  }

  def countBatch(
      batchType: String,
      batchUser: Option[String],
      batchState: Option[String] = None,
      kyuubiInstance: Option[String] = None): Int = {
    metadataManager.countBatch(
      batchType,
      batchUser.orNull,
      batchState.orNull,
      kyuubiInstance.orNull)
  }

  override def start(): Unit = {
    assert(running.compareAndSet(false, true))
    val submitTask: Runnable = () => {
      while (running.get) {
        metadataManager.pickBatchForSubmitting(kyuubiInstance) match {
          case None => Thread.sleep(1000)
          case Some(metadata) =>
            val batchId = metadata.identifier
            info(s"$batchId is picked for submission.")
            val batchSession = sessionManager.createBatchSession(
              metadata.username,
              "anonymous",
              metadata.ipAddress,
              metadata.requestConf,
              metadata.engineType,
              Option(metadata.requestName),
              metadata.resource,
              metadata.className,
              metadata.requestConf,
              metadata.requestArgs,
              Some(metadata), // TODO some logic need to fix since it's not from recovery
              shouldRunAsync = true)
            val metadataForUpdate = Metadata(
              identifier = batchId,
              kyuubiInstance = kyuubiInstance,
              requestConf = batchSession.optimizedConf,
              clusterManager = batchSession.batchJobSubmissionOp.builder.clusterManager())
            metadataManager.updateMetadata(metadataForUpdate, asyncRetryOnError = false)
            val sessionHandle = sessionManager.openBatchSession(batchSession)
            var submitted = false
            while (!submitted) { // block until batch job submitted
              submitted = sessionManager.getBatchSession(sessionHandle).map { batchSession =>
                val batchState = batchSession.batchJobSubmissionOp.getStatus.state
                batchState == OperationState.RUNNING || OperationState.isTerminal(batchState)
              }.getOrElse {
                error(s"Batch Session $batchId is not existed, marked as finished")
                true
              }
              // should we always treat metastore as the single of truth?
              //
              // submitted = metadataManager.getBatchSessionMetadata(batchId) match {
              //   case Some(metadata) =>
              //     val batchState = OperationState.withName(metadata.state)
              //     batchState == OperationState.RUNNING || OperationState.isTerminal(batchState)
              //   case None =>
              //     error(s"$batchId does not existed in metastore, assume it is finished")
              //     true
              // }
              if (!submitted) Thread.sleep(1000)
            }
            info(s"$batchId is submitted.")
        }
      }
    }
    (0 until batchExecutor.getCorePoolSize).foreach(_ => batchExecutor.submit(submitTask))
    super.start()
  }

  override def stop(): Unit = {
    super.stop()
    if (running.compareAndSet(true, false)) {
      ThreadUtils.shutdown(batchExecutor)
    }
  }
}
