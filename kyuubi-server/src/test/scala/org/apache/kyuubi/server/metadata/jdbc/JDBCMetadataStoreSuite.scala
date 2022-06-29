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

package org.apache.kyuubi.server.metadata.jdbc

import java.util.UUID

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.session.SessionType

class JDBCMetadataStoreSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.DERBY.toString)
    .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.connectionTimeout", "3000")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.maximumPoolSize", "99")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.idleTimeout", "60000")
  private val jdbcMetadataStore = new JDBCMetadataStore(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcMetadataStore.getMetadataList(
      MetadataFilter(),
      0,
      Int.MaxValue,
      true).foreach {
      batch =>
        jdbcMetadataStore.cleanupMetadataByIdentifier(batch.identifier)
    }
    jdbcMetadataStore.close()
  }

  test("test jdbc datasource properties") {
    assert(jdbcMetadataStore.hikariDataSource.getConnectionTimeout == 3000)
    assert(jdbcMetadataStore.hikariDataSource.getMaximumPoolSize == 99)
    assert(jdbcMetadataStore.hikariDataSource.getIdleTimeout == 60000)
  }

  test("jdbc metadata store") {
    val batchId = UUID.randomUUID().toString
    val kyuubiInstance = "localhost:10099"
    var batchMetadata = Metadata(
      identifier = batchId,
      sessionType = SessionType.BATCH,
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
      engineType = "spark",
      clusterManager = Some("local"))

    var batchStateOnlyMetadata = batchMetadata.copy(
      resource = null,
      className = null,
      requestConf = Map.empty,
      requestArgs = Seq.empty)

    jdbcMetadataStore.insertMetadata(batchMetadata)
    assert(jdbcMetadataStore.getMetadata(batchId, true) != batchStateOnlyMetadata)
    assert(jdbcMetadataStore.getMetadata(batchId, false) != batchMetadata)

    // the engine type is formatted with UPPER
    batchMetadata = batchMetadata.copy(engineType = "SPARK")
    batchStateOnlyMetadata = batchStateOnlyMetadata.copy(engineType = "SPARK")
    assert(jdbcMetadataStore.getMetadata(batchId, true) == batchStateOnlyMetadata)
    assert(jdbcMetadataStore.getMetadata(batchId, false) == batchMetadata)

    jdbcMetadataStore.cleanupMetadataByIdentifier(batchId)
    assert(jdbcMetadataStore.getMetadata(batchId, true) == null)

    jdbcMetadataStore.insertMetadata(batchMetadata)

    val batchState2 = batchStateOnlyMetadata.copy(identifier = UUID.randomUUID().toString)
    jdbcMetadataStore.insertMetadata(batchState2)

    var batches =
      jdbcMetadataStore.getMetadataList(
        MetadataFilter(
          sessionType = SessionType.BATCH,
          engineType = "Spark"),
        0,
        1,
        true)
    assert(batches == Seq(batchStateOnlyMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi"),
      0,
      Int.MaxValue,
      true)
    assert(batches == Seq(batchStateOnlyMetadata, batchState2))

    jdbcMetadataStore.cleanupMetadataByIdentifier(batchState2.identifier)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.SQL,
        engineType = "Spark",
        username = "kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue,
      true)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue,
      true)
    assert(batches == Seq(batchStateOnlyMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi",
        state = "RUNNING"),
      0,
      Int.MaxValue,
      true)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "no_kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue,
      true)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "SPARK",
        state = "PENDING"),
      0,
      Int.MaxValue,
      true)
    assert(batches == Seq(batchStateOnlyMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(sessionType = SessionType.BATCH),
      0,
      Int.MaxValue,
      true)
    assert(batches == Seq(batchStateOnlyMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        peerInstanceClosed = true),
      0,
      Int.MaxValue,
      true)
    assert(batches.isEmpty)

    jdbcMetadataStore.updateMetadata(Metadata(
      identifier = batchStateOnlyMetadata.identifier,
      peerInstanceClosed = true))

    batchStateOnlyMetadata = batchStateOnlyMetadata.copy(peerInstanceClosed = true)
    batchMetadata = batchMetadata.copy(peerInstanceClosed = true)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        peerInstanceClosed = true),
      0,
      Int.MaxValue,
      true)
    assert(batches === Seq(batchStateOnlyMetadata))

    var batchesToRecover = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "PENDING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue,
      false)
    assert(batchesToRecover == Seq(batchMetadata))

    batchesToRecover = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "RUNNING",
        kyuubiInstance = kyuubiInstance),
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
    jdbcMetadataStore.updateMetadata(newBatchState)
    assert(jdbcMetadataStore.getMetadata(batchId, true) == newBatchState)

    newBatchState = newBatchState.copy(state = "FINISHED", endTime = System.currentTimeMillis())
    jdbcMetadataStore.updateMetadata(newBatchState)

    assert(jdbcMetadataStore.getMetadata(batchId, true) == newBatchState)

    assert(jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "PENDING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue,
      false).isEmpty)

    assert(jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "RUNNING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue,
      false).isEmpty)

    eventually(Timeout(3.seconds)) {
      jdbcMetadataStore.cleanupMetadataByAge(1000)
      assert(jdbcMetadataStore.getMetadata(batchId, true) == null)
    }
  }

  test("throw exception if update count is 0") {
    val metadata = Metadata(identifier = UUID.randomUUID().toString, state = "RUNNING")
    intercept[KyuubiException] {
      jdbcMetadataStore.updateMetadata(metadata)
    }
  }
}
