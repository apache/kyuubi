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

import java.util.UUID

import scala.collection.JavaConverters._

import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf.METADATA_STORE_JDBC_PRIORITY_ENABLED
import org.apache.kyuubi.session.SessionType

class MetadataManagerSuite extends KyuubiFunSuite {

  test("fail fast on duplicated key") {
    Seq("true", "false").foreach { enableAsyncRetry =>
      withMetadataManager(Map(
        METADATA_REQUEST_ASYNC_RETRY_ENABLED.key -> enableAsyncRetry,
        METADATA_REQUEST_RETRY_INTERVAL.key -> "100")) { metadataManager =>
        val metadata = newMetadata()
        metadataManager.insertMetadata(metadata)
        Seq(true, false).foreach { asyncRetryOnError =>
          intercept[KyuubiException] {
            metadataManager.insertMetadata(metadata, asyncRetryOnError)
          }
        }
      }
    }
  }

  test("async retry the metadata store requests") {
    withMetadataManager(
      Map(
        METADATA_REQUEST_ASYNC_RETRY_ENABLED.key -> "true",
        METADATA_REQUEST_RETRY_INTERVAL.key -> "100"),
      () =>
        new MetadataManager {
          override protected def unrecoverableDBErr(cause: Throwable): Boolean = false
        }) { metadataManager =>
      val metadata = newMetadata()
      metadataManager.insertMetadata(metadata)
      metadataManager.insertMetadata(metadata, asyncRetryOnError = true)
      val retryRef = metadataManager.getMetadataRequestsRetryRef(metadata.identifier)
      val metadataToUpdate = metadata.copy(state = "RUNNING")
      retryRef.addRetryingMetadataRequest(UpdateMetadata(metadataToUpdate))
      eventually(timeout(3.seconds)) {
        assert(retryRef.hasRemainingRequests())
        assert(metadataManager.getBatch(metadata.identifier).map(_.getState).contains("PENDING"))
      }

      val metadata2 = metadata.copy(identifier = UUID.randomUUID().toString)
      val metadata2ToUpdate = metadata2.copy(
        engineId = "app_id",
        engineName = "app_name",
        engineUrl = "app_url",
        engineState = "app_state",
        state = "RUNNING")

      metadataManager.addMetadataRetryRequest(InsertMetadata(metadata2))
      metadataManager.addMetadataRetryRequest(UpdateMetadata(metadata2ToUpdate))

      val retryRef2 = metadataManager.getMetadataRequestsRetryRef(metadata2.identifier)

      eventually(timeout(3.seconds)) {
        assert(!retryRef2.hasRemainingRequests())
        assert(metadataManager.getBatch(metadata2.identifier).map(_.getState).contains("RUNNING"))
      }

      metadataManager.identifierRequestsAsyncRetryRefs.clear()
      eventually(timeout(3.seconds)) {
        metadataManager.identifierRequestsAsyncRetryingCounts.asScala.forall(_._2.get() == 0)
      }
      metadataManager.identifierRequestsAsyncRetryingCounts.clear()
    }
  }

  test("async metadata request metrics") {
    withMetadataManager(Map(
      METADATA_REQUEST_ASYNC_RETRY_ENABLED.key -> "true",
      METADATA_REQUEST_RETRY_INTERVAL.key -> "100")) { metadataManager =>
      val totalRequests = MetricsSystem.meterValue(METADATA_REQUEST_TOTAL).getOrElse(0L)
      val failedRequests = MetricsSystem.meterValue(METADATA_REQUEST_FAIL).getOrElse(0L)
      val retryingRequests = MetricsSystem.meterValue(METADATA_REQUEST_RETRYING).getOrElse(0L)

      val metadata = newMetadata()
      metadataManager.insertMetadata(metadata)

      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_TOTAL)
          .getOrElse(0L) - totalRequests === 1)
      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_FAIL)
          .getOrElse(0L) - failedRequests === 0)
      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_RETRYING)
          .getOrElse(0L) - retryingRequests === 0)

      val invalidMetadata = metadata.copy(state = null)
      intercept[Exception](metadataManager.insertMetadata(invalidMetadata, false))
      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_TOTAL)
          .getOrElse(0L) - totalRequests === 2)
      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_FAIL)
          .getOrElse(0L) - failedRequests === 1)
      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_RETRYING)
          .getOrElse(0L) - retryingRequests === 0)

      metadataManager.insertMetadata(invalidMetadata, true)

      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_TOTAL)
          .getOrElse(0L) - totalRequests === 3)
      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_FAIL)
          .getOrElse(0L) - failedRequests === 2)
      assert(
        MetricsSystem.meterValue(MetricsConstants.METADATA_REQUEST_RETRYING)
          .getOrElse(0L) - retryingRequests === 1)
    }
  }

  test("[KYUUBI #5328] Test MetadataManager#pickBatchForSubmitting in order") {
    // build mock batch jobs
    val mockKyuubiInstance = "mock_kyuubi_instance"
    val time = System.currentTimeMillis()
    val mockBatchJob1 = newMetadata(
      identifier = "mock_batch_job_1",
      state = OperationState.INITIALIZED.toString,
      createTime = time + 10000,
      // larger than default priority 10
      priority = 20)
    val mockBatchJob2 = newMetadata(
      identifier = "mock_batch_job_2",
      state = OperationState.INITIALIZED.toString,
      createTime = time)
    val mockBatchJob3 = newMetadata(
      identifier = "mock_batch_job_3",
      state = OperationState.INITIALIZED.toString,
      createTime = time + 5000)

    withMetadataManager(Map(METADATA_STORE_JDBC_PRIORITY_ENABLED.key -> "true")) {
      metadataManager =>
        metadataManager.insertMetadata(mockBatchJob1, asyncRetryOnError = false)
        metadataManager.insertMetadata(mockBatchJob2, asyncRetryOnError = false)
        metadataManager.insertMetadata(mockBatchJob3, asyncRetryOnError = false)

        // pick the highest priority batch job
        val metadata1 = metadataManager.pickBatchForSubmitting(mockKyuubiInstance)
        assert(metadata1.exists(m => m.identifier === "mock_batch_job_1"))

        // pick the oldest batch job when same priority
        val metadata2 = metadataManager.pickBatchForSubmitting(mockKyuubiInstance)
        assert(metadata2.exists(m => m.identifier === "mock_batch_job_2"))

        val metadata3 = metadataManager.pickBatchForSubmitting(mockKyuubiInstance)
        assert(metadata3.exists(m => m.identifier === "mock_batch_job_3"))
    }

    withMetadataManager(Map(METADATA_STORE_JDBC_PRIORITY_ENABLED.key -> "false")) {
      metadataManager =>
        metadataManager.insertMetadata(mockBatchJob1, asyncRetryOnError = false)
        metadataManager.insertMetadata(mockBatchJob2, asyncRetryOnError = false)
        metadataManager.insertMetadata(mockBatchJob3, asyncRetryOnError = false)

        // pick the oldest batch job
        val metadata2 = metadataManager.pickBatchForSubmitting(mockKyuubiInstance)
        assert(metadata2.exists(m => m.identifier === "mock_batch_job_2"))

        val metadata3 = metadataManager.pickBatchForSubmitting(mockKyuubiInstance)
        assert(metadata3.exists(m => m.identifier === "mock_batch_job_3"))
    }
  }

  private def withMetadataManager(
      confOverlay: Map[String, String],
      newMetadataMgr: () => MetadataManager = () => new MetadataManager())(
      f: MetadataManager => Unit): Unit = {
    val metricsSystem = new MetricsSystem()
    val metadataManager = newMetadataMgr()
    val conf = KyuubiConf()
    confOverlay.foreach { case (k, v) => conf.set(k, v) }
    try {
      metricsSystem.initialize(conf)
      metricsSystem.start()
      metadataManager.initialize(conf)
      metadataManager.start()
      f(metadataManager)
    } finally {
      metadataManager.getBatches(MetadataFilter(), 0, Int.MaxValue).foreach { batch =>
        // close the batch if not ended
        if (batch.getEndTime == 0) {
          metadataManager.updateMetadata(
            Metadata(
              identifier = batch.getId,
              state = OperationState.CLOSED.toString,
              endTime = System.currentTimeMillis()),
            false)
        }
      }

      metadataManager.cleanupMetadata(0, 1, 0)

      // ensure all metadata are cleaned up
      eventually(timeout(3.seconds), interval(200.milliseconds)) {
        assert(metadataManager.getBatches(MetadataFilter(), 0, Int.MaxValue).isEmpty)
      }

      // ensure no metadata request leak
      eventually(timeout(5.seconds), interval(200.milliseconds)) {
        assert(MetricsSystem.counterValue(METADATA_REQUEST_OPENED).getOrElse(0L) === 0)
      }
      metadataManager.stop()
      metricsSystem.stop()
    }
  }

  private def newMetadata(
      identifier: String = UUID.randomUUID().toString,
      state: String = OperationState.PENDING.toString,
      createTime: Long = System.currentTimeMillis(),
      priority: Int = 10): Metadata = {
    Metadata(
      identifier = identifier,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "127.0.0.1",
      kyuubiInstance = "localhost:10009",
      state = state,
      resource = "intern",
      className = "org.apache.kyuubi.SparkWC",
      requestName = "kyuubi_batch",
      requestConf = Map("spark.master" -> "local"),
      requestArgs = Seq("100"),
      createTime = createTime,
      engineType = "spark",
      priority = priority,
      clusterManager = Some("local"))
  }
}
