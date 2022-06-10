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

import java.util.UUID

import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.statestore.api.SessionMetadata
import org.apache.kyuubi.session.SessionType

class StateStoreRequestsRetryManagerSuite extends KyuubiFunSuite {
  val sessionStateStore = new SessionStateStore()
  val conf = KyuubiConf().set(KyuubiConf.SERVER_STATE_STORE_REQUESTS_RETRY_INTERVAL, 100L)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sessionStateStore.initialize(conf)
    sessionStateStore.start()
  }

  override def afterAll(): Unit = {
    sessionStateStore.getBatches(null, null, null, 0, 0, 0, Int.MaxValue).foreach { batch =>
      sessionStateStore.cleanupMetadataById(batch.getId)
    }
    sessionStateStore.stop()
    super.afterAll()
  }

  test("retry the state store requests") {
    val metadata = SessionMetadata(
      identifier = UUID.randomUUID().toString,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "127.0.0.1",
      kyuubiInstance = "localhost:10009",
      state = "PENDING",
      resource = "intern",
      className = "org.apache.kyuubi.SparkWC",
      requestName = "kyuubi_batch",
      requestConf = Map("spark.master" -> "local"),
      requestArgs = Seq("100"),
      createTime = System.currentTimeMillis(),
      engineType = "spark",
      clusterManager = Some("local"))
    sessionStateStore.insertMetadata(metadata)
    intercept[KyuubiException] {
      sessionStateStore.insertMetadata(metadata, retryOnError = false)
    }
    sessionStateStore.insertMetadata(metadata, retryOnError = true)
    val retryRef = sessionStateStore.getRequestsRetryRef(metadata.identifier)
    val metadataToUpdate = metadata.copy(state = "RUNNING")
    retryRef.addRetryingSessionStateRequest(UpdateMetadata(metadataToUpdate))
    eventually(timeout(3.seconds)) {
      assert(retryRef.hasRemainingRequests())
      assert(retryRef.retryCount.get() > 1)
      assert(sessionStateStore.getBatch(metadata.identifier).getState === "PENDING")
    }

    val metadata2 = metadata.copy(identifier = UUID.randomUUID().toString)
    val metadata2ToUpdate = metadata2.copy(
      engineId = "app_id",
      engineName = "app_name",
      engineUrl = "app_url",
      engineState = "app_state",
      state = "RUNNING")

    val retryRef2 = sessionStateStore.requestsRetryManager
      .getOrCreateStateStoreRequestsRetryRef(metadata2.identifier)
    retryRef2.addRetryingSessionStateRequest(InsertMetadata(metadata2))
    retryRef2.addRetryingSessionStateRequest(UpdateMetadata(metadata2ToUpdate))

    eventually(timeout(3.seconds)) {
      assert(!retryRef2.hasRemainingRequests())
      assert(sessionStateStore.getBatch(metadata2.identifier).getState === "RUNNING")
      assert(retryRef2.retryCount.get() === 1)
      assert(retryRef2.retryingTaskCount.get() === 0)
    }
  }
}
