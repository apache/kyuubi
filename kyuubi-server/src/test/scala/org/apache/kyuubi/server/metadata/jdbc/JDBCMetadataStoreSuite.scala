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
import org.apache.kyuubi.engine.ApplicationState
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.{KubernetesEngineInfo, Metadata, MetadataFilter}
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.session.SessionType

class JDBCMetadataStoreSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.SQLITE.toString)
    .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.connectionTimeout", "3000")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.maximumPoolSize", "99")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.idleTimeout", "60000")
  private val jdbcMetadataStore = new JDBCMetadataStore(conf)

  override def afterAll(): Unit = {
    super.afterAll()
    jdbcMetadataStore.getMetadataList(MetadataFilter(), 0, Int.MaxValue).foreach {
      batch =>
        jdbcMetadataStore.cleanupMetadataByIdentifier(batch.identifier)
    }
    jdbcMetadataStore.cleanupKubernetesEngineInfoByAge(0)
    jdbcMetadataStore.close()
  }

  test("test jdbc datasource properties") {
    assert(jdbcMetadataStore.hikariDataSource.getConnectionTimeout == 3000)
    assert(jdbcMetadataStore.hikariDataSource.getMaximumPoolSize == 99)
    assert(jdbcMetadataStore.hikariDataSource.getIdleTimeout == 60000)
  }

  test("test get init schema stream") {
    assert(jdbcMetadataStore.getInitSchema(DatabaseType.MYSQL).isDefined)
    assert(jdbcMetadataStore.getInitSchema(DatabaseType.POSTGRESQL).isDefined)
    assert(jdbcMetadataStore.getInitSchema(DatabaseType.CUSTOM).isEmpty)
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

    jdbcMetadataStore.insertMetadata(batchMetadata)

    // the engine type is formatted with UPPER
    batchMetadata = batchMetadata.copy(engineType = "SPARK")
    assert(jdbcMetadataStore.getMetadata(batchId) == batchMetadata)

    jdbcMetadataStore.cleanupMetadataByIdentifier(batchId)
    assert(jdbcMetadataStore.getMetadata(batchId) == null)

    jdbcMetadataStore.insertMetadata(batchMetadata)

    val batchState2 = batchMetadata.copy(identifier = UUID.randomUUID().toString)
    jdbcMetadataStore.insertMetadata(batchState2)

    var batches =
      jdbcMetadataStore.getMetadataList(
        MetadataFilter(
          sessionType = SessionType.BATCH,
          engineType = "Spark"),
        0,
        1)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi"),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata, batchState2))

    jdbcMetadataStore.cleanupMetadataByIdentifier(batchState2.identifier)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.INTERACTIVE,
        engineType = "Spark",
        username = "kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "kyuubi",
        state = "RUNNING"),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "Spark",
        username = "no_kyuubi",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        engineType = "SPARK",
        state = "PENDING"),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(sessionType = SessionType.BATCH),
      0,
      Int.MaxValue)
    assert(batches == Seq(batchMetadata))

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        peerInstanceClosed = true),
      0,
      Int.MaxValue)
    assert(batches.isEmpty)

    jdbcMetadataStore.updateMetadata(Metadata(
      identifier = batchMetadata.identifier,
      peerInstanceClosed = true))

    batchMetadata = batchMetadata.copy(peerInstanceClosed = true)

    batches = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        peerInstanceClosed = true),
      0,
      Int.MaxValue)
    assert(batches === Seq(batchMetadata))

    var batchesToRecover = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "PENDING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue)
    assert(batchesToRecover == Seq(batchMetadata))

    batchesToRecover = jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "RUNNING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue)
    assert(batchesToRecover.isEmpty)

    var newBatchState = batchMetadata.copy(
      state = "RUNNING",
      engineId = "app_id",
      engineName = "app_name",
      engineUrl = "app_url",
      engineState = "RUNNING",
      engineError = None)
    jdbcMetadataStore.updateMetadata(newBatchState)
    assert(jdbcMetadataStore.getMetadata(batchId) == newBatchState)

    newBatchState = newBatchState.copy(state = "FINISHED", endTime = System.currentTimeMillis())
    jdbcMetadataStore.updateMetadata(newBatchState)

    assert(jdbcMetadataStore.getMetadata(batchId) == newBatchState)

    assert(jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "PENDING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue).isEmpty)

    assert(jdbcMetadataStore.getMetadataList(
      MetadataFilter(
        sessionType = SessionType.BATCH,
        state = "RUNNING",
        kyuubiInstance = kyuubiInstance),
      0,
      Int.MaxValue).isEmpty)

    eventually(Timeout(3.seconds)) {
      jdbcMetadataStore.cleanupMetadataByAge(1000)
      assert(jdbcMetadataStore.getMetadata(batchId) == null)
    }
  }

  test("throw exception if update count is 0") {
    val metadata = Metadata(identifier = UUID.randomUUID().toString, state = "RUNNING")
    intercept[KyuubiException] {
      jdbcMetadataStore.updateMetadata(metadata)
    }
  }

  test("get schema urls with correct version ordering") {
    val url1 = "metadata-store-schema-1.7.0.mysql.sql"
    val url2 = "metadata-store-schema-1.7.1.mysql.sql"
    val url3 = "metadata-store-schema-1.8.0.mysql.sql"
    val url4 = "metadata-store-schema-1.10.0.mysql.sql"
    val url5 = "metadata-store-schema-2.1.0.mysql.sql"
    assert(jdbcMetadataStore.getSchemaVersion(url1) === ((1, 7, 0)))
    assert(jdbcMetadataStore.getSchemaVersion(url2) === ((1, 7, 1)))
    assert(jdbcMetadataStore.getSchemaVersion(url3) === ((1, 8, 0)))
    assert(jdbcMetadataStore.getSchemaVersion(url4) === ((1, 10, 0)))
    assert(jdbcMetadataStore.getSchemaVersion(url5) === ((2, 1, 0)))
    assert(jdbcMetadataStore.getLatestSchemaUrl(Seq(url1, url2, url3, url4)).get === url4)
    assert(jdbcMetadataStore.getLatestSchemaUrl(Seq(url1, url3, url4, url2)).get === url4)
    assert(jdbcMetadataStore.getLatestSchemaUrl(Seq(url1, url2, url3, url4, url5)).get === url5)
  }

  test("kubernetes engine info") {
    val tag = UUID.randomUUID().toString
    val metadata = KubernetesEngineInfo(
      identifier = tag,
      context = Some("context"),
      namespace = Some("namespace"),
      podName = "podName",
      podState = "podState",
      containerState = "containerState",
      engineId = "appId",
      engineName = "appName",
      engineState = "FINISHED",
      engineError = Some("appError"))

    jdbcMetadataStore.upsertKubernetesEngineInfo(metadata)

    val metadata2 = jdbcMetadataStore.getKubernetesMetaEngineInfo(tag)
    assert(metadata2.identifier == metadata.identifier)
    assert(metadata2.context == metadata.context)
    assert(metadata2.namespace == metadata.namespace)
    assert(metadata2.podName == metadata.podName)
    assert(metadata2.podState == metadata.podState)
    assert(metadata2.containerState == metadata.containerState)
    assert(metadata2.engineId == metadata.engineId)
    assert(metadata2.engineName == metadata.engineName)
    assert(metadata2.engineState == metadata.engineState)
    assert(metadata2.engineError == metadata.engineError)
    assert(metadata2.updateTime > 0)

    val metadata3 = KubernetesEngineInfo(
      identifier = tag,
      context = Some("context2"),
      namespace = Some("namespace2"),
      podName = "podName2",
      podState = "podState2",
      containerState = "containerState2",
      engineId = "appId2",
      engineName = "appName2",
      engineState = "FAILED",
      engineError = Some("appError2"))
    jdbcMetadataStore.upsertKubernetesEngineInfo(metadata3)

    val metadata4 = jdbcMetadataStore.getKubernetesMetaEngineInfo(tag)
    assert(metadata4.identifier == metadata3.identifier)
    assert(metadata4.context == metadata3.context)
    assert(metadata4.namespace == metadata3.namespace)
    assert(metadata4.podName == metadata3.podName)
    assert(metadata4.podState == metadata3.podState)
    assert(metadata4.containerState == metadata3.containerState)
    assert(metadata4.engineId == metadata3.engineId)
    assert(metadata4.engineName == metadata3.engineName)
    assert(metadata4.engineState == metadata3.engineState)
    assert(metadata4.engineError == metadata3.engineError)
    assert(metadata4.updateTime > metadata2.updateTime)

    val applicationInfo =
      MetadataManager.buildApplicationInfo(jdbcMetadataStore.getKubernetesMetaEngineInfo(tag))
    assert(applicationInfo.id == "appId2")
    assert(applicationInfo.name == "appName2")
    assert(applicationInfo.state == ApplicationState.FAILED)
    assert(applicationInfo.error == Some("appError2"))
    assert(applicationInfo.podName == Some("podName2"))

    jdbcMetadataStore.cleanupKubernetesEngineInfoByIdentifier(tag)
    assert(jdbcMetadataStore.getKubernetesMetaEngineInfo(tag) == null)
  }
}
