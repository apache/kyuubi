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
import org.apache.kyuubi.server.statestore.api.{BatchMeta, BatchState}

class JDBCStateStoreSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(SERVER_STATE_STORE_JDBC_DB_TYPE, DataBaseType.DERBY.toString)
    .set(SERVER_STATE_STORE_JDBC_DB_SCHEMA_INIT, true)
    .set(SERVER_STATE_STORE_MAX_AGE, 3000L)
  private val jdbcStateStore = new JDBCStateStore(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcStateStore.shutdown()
  }

  test("jdbc state store") {
    val batchId = UUID.randomUUID().toString
    val batchState = BatchState(
      batchId,
      "spark",
      "kyuubi",
      "localhost:10099",
      "PENDING",
      System.currentTimeMillis())
    jdbcStateStore.createBatch(batchState)
    assert(jdbcStateStore.getBatch(batchId) == batchState)

    val batchMeta = BatchMeta(
      batchId,
      "127.0.0.1",
      Map("kyuubi.test" -> "true"),
      "spark",
      "intern",
      "org.apache.kyuubi.SparkWC",
      "kyuubi_batch",
      Map("spark.master" -> "local"),
      Seq("100"))
    jdbcStateStore.saveBatchMeta(batchMeta)
    assert(jdbcStateStore.getBatchMeta(batchId) == batchMeta)

    jdbcStateStore.cleanupBatch(batchId)
    assert(jdbcStateStore.getBatch(batchId) == null)
    assert(jdbcStateStore.getBatchMeta(batchId) == null)

    jdbcStateStore.createBatch(batchState)
    jdbcStateStore.saveBatchMeta(batchMeta)

    val batches = jdbcStateStore.getBatches("SPARK", null, null, 0, Int.MaxValue)
    assert(batches == Seq(batchState))

    val batchesToRecover = jdbcStateStore.getBatchesToRecover("localhost:10099", 0, Int.MaxValue)
    assert(batchesToRecover == Seq(batchState))

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
    jdbcStateStore.closeBatch(batchId, newBatchState.state, newBatchState.endTime)
    assert(jdbcStateStore.getBatch(batchId) == newBatchState)

    eventually(Timeout(5.seconds)) {
      jdbcStateStore.checkAndCleanupBatches()
      assert(jdbcStateStore.getBatch(batchId) == null)
      assert(jdbcStateStore.getBatchMeta(batchId) == null)
    }
  }
}
