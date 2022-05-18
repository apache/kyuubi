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
import org.apache.kyuubi.server.statestore.api.Metadata

class JDBCStateStoreSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(SERVER_STATE_STORE_JDBC_DATABASE_TYPE, DatabaseType.DERBY.toString)
    .set(SERVER_STATE_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
    .set(s"$KYUUBI_STATE_STORE_JDBC_DATASOURCE_PREFIX.connectionTimeout", "3000")
    .set(s"$KYUUBI_STATE_STORE_JDBC_DATASOURCE_PREFIX.maximumPoolSize", "99")
    .set(s"$KYUUBI_STATE_STORE_JDBC_DATASOURCE_PREFIX.idleTimeout", "60000")
  private val jdbcStateStore = new JDBCStateStore(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcStateStore.getMetadataList(null, null, null, null, 0, Int.MaxValue, true).foreach { batch =>
      jdbcStateStore.cleanupMetadataByIdentifier(batch.identifier)
    }
    jdbcStateStore.close()
  }

  test("test jdbc datasource properties") {
    assert(jdbcStateStore.hikariDataSource.getConnectionTimeout == 3000)
    assert(jdbcStateStore.hikariDataSource.getMaximumPoolSize == 99)
    assert(jdbcStateStore.hikariDataSource.getIdleTimeout == 60000)
  }

  test("jdbc state store") {
    val batchId = UUID.randomUUID().toString
    val kyuubiInstance = "localhost:10099"
    val batchMetadata = Metadata(
      identifier = batchId,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "127.0.0.1",
      kyuubiInstance = kyuubiInstance,
      state = "PENDING",
      resource = "intern",
      className = "org.apache.kyuubi.SparkWC",
      requestName = "kyuubi_batch",
      requestConf = Map("spark.master" -> "local"),
      requestArgs = Seq("100"),
      createTime = System.currentTimeMillis(),
      engineType = "spark")

    val batchStateOnlyMetadata = batchMetadata.copy(
      resource = null,
      className = null,
      requestConf = Map.empty,
      requestArgs = Seq.empty)

    jdbcStateStore.insertMetadata(batchMetadata)
    assert(jdbcStateStore.getMetadata(batchId, true) == batchStateOnlyMetadata)
    assert(jdbcStateStore.getMetadata(batchId, false) == batchMetadata)

    jdbcStateStore.cleanupMetadataByIdentifier(batchId)
    assert(jdbcStateStore.getMetadata(batchId, true) == null)

    jdbcStateStore.insertMetadata(batchMetadata)

    val batchState2 = batchStateOnlyMetadata.copy(identifier = UUID.randomUUID().toString)
    jdbcStateStore.insertMetadata(batchState2)

    var batches = jdbcStateStore.getMetadataList("Spark", null, null, null, 0, 1, true)
    assert(batches == Seq(batchStateOnlyMetadata))

    batches = jdbcStateStore.getMetadataList("spark", "kyuubi", null, null, 0, Int.MaxValue, true)
    assert(batches == Seq(batchStateOnlyMetadata, batchState2))

    jdbcStateStore.cleanupMetadataByIdentifier(batchState2.identifier)

    batches =
      jdbcStateStore.getMetadataList("SPARK", "kyuubi", "PENDING", null, 0, Int.MaxValue, true)
    assert(batches == Seq(batchStateOnlyMetadata))

    batches =
      jdbcStateStore.getMetadataList("SPARK", "kyuubi", "RUNNING", null, 0, Int.MaxValue, true)
    assert(batches.isEmpty)

    batches =
      jdbcStateStore.getMetadataList("SPARK", "no_kyuubi", "PENDING", null, 0, Int.MaxValue, true)
    assert(batches.isEmpty)

    batches = jdbcStateStore.getMetadataList("SPARK", null, "PENDING", null, 0, Int.MaxValue, true)
    assert(batches == Seq(batchStateOnlyMetadata))

    batches = jdbcStateStore.getMetadataList(null, null, null, null, 0, Int.MaxValue, true)
    assert(batches == Seq(batchStateOnlyMetadata))

    var batchesToRecover = jdbcStateStore.getMetadataList(
      null,
      null,
      "PENDING",
      kyuubiInstance,
      0,
      Int.MaxValue,
      false)
    assert(batchesToRecover == Seq(batchMetadata))

    batchesToRecover = jdbcStateStore.getMetadataList(
      null,
      null,
      "RUNNING",
      kyuubiInstance,
      0,
      Int.MaxValue,
      false)
    assert(batchesToRecover.isEmpty)

    var newBatchState = batchStateOnlyMetadata.copy(
      state = "RUNNING",
      engineId = "app_id",
      engineName = "app_name",
      engineUrl = "app_url",
      engineState = "RUNNING",
      engineError = None)
    jdbcStateStore.updateMetadata(newBatchState)
    assert(jdbcStateStore.getMetadata(batchId, true) == newBatchState)

    newBatchState = newBatchState.copy(state = "FINISHED", endTime = System.currentTimeMillis())
    jdbcStateStore.updateMetadata(newBatchState)

    assert(jdbcStateStore.getMetadata(batchId, true) == newBatchState)

    assert(jdbcStateStore.getMetadataList(
      null,
      null,
      "PENDING",
      kyuubiInstance,
      0,
      Int.MaxValue,
      false).isEmpty)

    assert(jdbcStateStore.getMetadataList(
      null,
      null,
      "RUNNING",
      kyuubiInstance,
      0,
      Int.MaxValue,
      false).isEmpty)

    eventually(Timeout(3.seconds)) {
      jdbcStateStore.cleanupMetadataByAge(1000)
      assert(jdbcStateStore.getMetadata(batchId, true) == null)
    }
  }
}
