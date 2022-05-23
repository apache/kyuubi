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

package org.apache.kyuubi.server.statestore.jdbc

import java.util.UUID

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.statestore.api.BatchMetadata

class JDBCStateStoreSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(SERVER_STATE_STORE_JDBC_DB_TYPE, DatabaseType.DERBY.toString)
    .set(SERVER_STATE_STORE_JDBC_DB_SCHEMA_INIT, true)
    .set(SERVER_STATE_STORE_MAX_AGE, 3000L)
  private val jdbcStateStore = new JDBCStateStore(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcStateStore.shutdown()
  }

  test("jdbc state store") {
    val batchId = UUID.randomUUID().toString
    val kyuubiInstance = "localhost:10099"
    val batch = BatchMetadata(
      batchId,
      "kyuubi",
      "localhost",
      Map("kyuubi.test" -> "true"),
      kyuubiInstance,
      "SPARK",
      "intern",
      "org.apache.kyuubi.SparkWC",
      "kyuubi_batch",
      Map("spark.master" -> "local"),
      Seq("100"),
      "PENDING",
      System.currentTimeMillis())

    val batchState = batch.copy(
      ipAddress = null,
      sessionConf = Map.empty,
      resource = null,
      className = null,
      name = null,
      conf = Map.empty,
      args = Seq.empty)

    jdbcStateStore.createBatch(batch)
    assert(jdbcStateStore.getBatch(batchId) == batchState)

    jdbcStateStore.cleanupBatch(batchId)
    assert(jdbcStateStore.getBatch(batchId) == null)

    jdbcStateStore.createBatch(batch)

    var batches = jdbcStateStore.getBatches("SPARK", null, null, 0, Int.MaxValue)
    assert(batches == Seq(batchState))

    batches = jdbcStateStore.getBatches("SPARK", "kyuubi", null, 0, Int.MaxValue)
    assert(batches == Seq(batchState))

    batches = jdbcStateStore.getBatches("SPARK", "kyuubi", "PENDING", 0, Int.MaxValue)
    assert(batches == Seq(batchState))

    batches = jdbcStateStore.getBatches("SPARK", "kyuubi", "RUNNING", 0, Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcStateStore.getBatches("SPARK", "no_kyuubi", "PENDING", 0, Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcStateStore.getBatches("SPARK", null, "PENDING", 0, Int.MaxValue)
    assert(batches == Seq(batchState))

    batches = jdbcStateStore.getBatches(null, null, null, 0, Int.MaxValue)
    assert(batches == Seq(batchState))

    val batchesToRecover = jdbcStateStore.getBatchesToRecover(kyuubiInstance, 0, Int.MaxValue)
    assert(batchesToRecover == Seq(batch))

    var newBatchState = batchState.copy(
      appId = "app_id",
      appName = "app_name",
      appUrl = "app_url",
      appState = "RUNNING",
      appError = None)
    jdbcStateStore.updateBatchAppInfo(
      batchId,
      newBatchState.appId,
      newBatchState.appName,
      newBatchState.appUrl,
      newBatchState.appState,
      newBatchState.appError)
    assert(jdbcStateStore.getBatch(batchId) == newBatchState)

    newBatchState = newBatchState.copy(state = "FINISHED", endTime = System.currentTimeMillis())
    jdbcStateStore.closeBatch(
      batchId,
      newBatchState.state,
      newBatchState.endTime,
      "app_id",
      "app_name",
      "app_url",
      "FINISHED",
      None)

    newBatchState = newBatchState.copy(appState = "FINISHED")
    assert(jdbcStateStore.getBatch(batchId) == newBatchState)

    assert(jdbcStateStore.getBatchesToRecover(kyuubiInstance, 0, Int.MaxValue).isEmpty)

    eventually(Timeout(5.seconds)) {
      jdbcStateStore.checkAndCleanupBatches()
      assert(jdbcStateStore.getBatch(batchId) == null)
    }
  }
}
