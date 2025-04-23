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

package org.apache.kyuubi.server.api.v1

import java.net.InetAddress
import java.nio.file.Paths
import java.util.UUID
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

import org.glassfish.jersey.media.multipart.FormDataMultiPart
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart

import org.apache.kyuubi.{BatchTestHelper, KyuubiFunSuite, RestFrontendTestHelper, Utils}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.client.util.BatchUtils
import org.apache.kyuubi.client.util.BatchUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationManagerInfo, KyuubiApplicationManager}
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.{BatchJobSubmission, OperationState}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.http.util.HttpAuthUtils.{basicAuthorizationHeader, AUTHORIZATION_HEADER}
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.service.authentication.{AnonymousAuthenticationProviderImpl, AuthUtils}
import org.apache.kyuubi.session.{KyuubiBatchSession, KyuubiSessionManager, SessionHandle, SessionType}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class BatchesV1ResourceSuite extends BatchesResourceSuiteBase {
  override def batchVersion: String = "1"

  override def customConf: Map[String, String] = Map.empty
}

class BatchesV2ResourceSuite extends BatchesResourceSuiteBase {
  override def batchVersion: String = "2"

  override def customConf: Map[String, String] = Map(
    METADATA_REQUEST_ASYNC_RETRY_ENABLED.key -> "false",
    BATCH_SUBMITTER_ENABLED.key -> "true")

  override def afterEach(): Unit = {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val batchService = fe.asInstanceOf[KyuubiRestFrontendService].batchService.get
    sessionManager.getBatchesFromMetadataStore(MetadataFilter(), 0, Int.MaxValue)
      .foreach { batch => batchService.cancelUnscheduledBatch(batch.getId) }
    super.afterEach()
    sessionManager.allSessions().foreach { session =>
      Utils.tryLogNonFatalError { sessionManager.closeSession(session.handle) }
    }
  }
}

abstract class BatchesResourceSuiteBase extends KyuubiFunSuite
  with RestFrontendTestHelper
  with BatchTestHelper {

  def batchVersion: String

  def customConf: Map[String, String]

  override protected lazy val conf: KyuubiConf = {
    val testResourceDir = Paths.get(sparkBatchTestResource.get).getParent
    val kyuubiConf = KyuubiConf()
      .set(AUTHENTICATION_METHOD, Seq("CUSTOM"))
      .set(AUTHENTICATION_CUSTOM_CLASS, classOf[AnonymousAuthenticationProviderImpl].getName)
      .set(SERVER_ADMINISTRATORS, Set("admin"))
      .set(BATCH_IMPL_VERSION, batchVersion)
      .set(SESSION_LOCAL_DIR_ALLOW_LIST, Set(testResourceDir.toString))
    customConf.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  override def afterEach(): Unit = {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach { session =>
      sessionManager.closeSession(session.handle)
    }
    sessionManager.getBatchesFromMetadataStore(MetadataFilter(), 0, Int.MaxValue).foreach { batch =>
      sessionManager.applicationManager.killApplication(ApplicationManagerInfo(None), batch.getId)
      sessionManager.cleanupMetadata(batch.getId)
    }
  }

  test("open batch session") {
    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 200)
    var batch = response.readEntity(classOf[Batch])
    batchVersion match {
      case "1" =>
        assert(batch.getKyuubiInstance === fe.connectionUrl)
      case "2" if batch.getState === "INITIALIZED" =>
        assert(batch.getKyuubiInstance === null)
      case "2" if batch.getState === "PENDING" => // batch picked by BatchService
        assert(batch.getKyuubiInstance === fe.connectionUrl)
      case _ =>
        fail(s"unexpected batch info, version: $batchVersion state: ${batch.getState}")
    }
    assert(batch.getBatchType === "SPARK")
    assert(batch.getName === sparkBatchTestAppName)
    assert(batch.getCreateTime > 0)
    assert(batch.getEndTime === 0)

    requestObj.setConf((requestObj.getConf.asScala ++
      Map(AuthUtils.HS2_PROXY_USER -> "root")).asJava)
    val proxyUserRequest = requestObj
    val proxyUserResponse = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .post(Entity.entity(proxyUserRequest, MediaType.APPLICATION_JSON_TYPE))
    assert(proxyUserResponse.getStatus === 403)
    var errorMessage = "Failed to validate proxy privilege of anonymous for root"
    assert(proxyUserResponse.readEntity(classOf[String]).contains(errorMessage))

    var getBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    assert(getBatchResponse.getStatus === 200)
    batch = getBatchResponse.readEntity(classOf[Batch])
    batchVersion match {
      case "1" =>
        assert(batch.getKyuubiInstance === fe.connectionUrl)
      case "2" if batch.getState === "INITIALIZED" =>
        assert(batch.getKyuubiInstance === null)
      case "2" if batch.getState === "PENDING" => // batch picked by BatchService
        assert(batch.getKyuubiInstance === fe.connectionUrl)
      case _ =>
        fail(s"unexpected batch info, version: $batchVersion state: ${batch.getState}")
    }
    assert(batch.getBatchType === "SPARK")
    assert(batch.getName === sparkBatchTestAppName)
    assert(batch.getCreateTime > 0)
    assert(batch.getEndTime === 0)
    if (batch.getAppId != null) {
      assert(batch.getAppStartTime > 0)
    }

    // invalid batchId
    getBatchResponse = webTarget.path(s"api/v1/batches/invalidBatchId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    assert(getBatchResponse.getStatus === 404)

    // get batch log
    var logResponse: Response = null
    var log: OperationLog = null
    eventually(timeout(10.seconds), interval(1.seconds)) {
      logResponse = webTarget.path(s"api/v1/batches/${batch.getId}/localLog")
        .queryParam("from", "0")
        .queryParam("size", "1")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
        .get()
      log = logResponse.readEntity(classOf[OperationLog])
      assert(log.getRowCount === 1)
    }
    val head = log.getLogRowSet.asScala.head

    val logs = new ArrayBuffer[String]
    logs.append(head)
    eventually(timeout(10.seconds), interval(1.seconds)) {
      logResponse = webTarget.path(s"api/v1/batches/${batch.getId}/localLog")
        .queryParam("from", "-1")
        .queryParam("size", "100")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
        .get()
      log = logResponse.readEntity(classOf[OperationLog])
      if (log.getRowCount > 0) {
        log.getLogRowSet.asScala.foreach(logs.append(_))
      }

      // check both kyuubi log and engine log
      assert(
        logs.exists(_.contains("bin/spark-submit")) &&
          logs.exists(_.contains(s"Submitted application: $sparkBatchTestAppName")))
    }

    // invalid user name
    var deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(batch.getId))
      .delete()
    assert(deleteBatchResponse.getStatus === 403)
    errorMessage = s"Failed to validate proxy privilege of ${batch.getId} for anonymous"
    assert(deleteBatchResponse.readEntity(classOf[String]).contains(errorMessage))

    // invalid batchId
    deleteBatchResponse = webTarget.path(s"api/v1/batches/notValidUUID")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .delete()
    assert(deleteBatchResponse.getStatus === 404)

    // non-existed batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${UUID.randomUUID().toString}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .delete()
    assert(deleteBatchResponse.getStatus === 404)

    // check close batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .delete()
    assert(deleteBatchResponse.getStatus === 200)
    val closeBatchResponse = deleteBatchResponse.readEntity(classOf[CloseBatchResponse])

    // check state after close batch session
    getBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    assert(getBatchResponse.getStatus === 200)
    batch = getBatchResponse.readEntity(classOf[Batch])
    assert(batch.getId === batch.getId)
    if (closeBatchResponse.isSuccess) {
      assert(batch.getState === "CANCELED")
    } else {
      assert(batch.getState != "CANCELED")
    }

    // close the closed batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .delete()
    assert(deleteBatchResponse.getStatus === 200)
    assert(!deleteBatchResponse.readEntity(classOf[CloseBatchResponse]).isSuccess)
  }

  test("open batch session with uploading resource") {
    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))
    val exampleJarFile = Paths.get(sparkBatchTestResource.get).toFile
    val multipart = new FormDataMultiPart()
      .field("batchRequest", requestObj, MediaType.APPLICATION_JSON_TYPE)
      .bodyPart(new FileDataBodyPart("resourceFile", exampleJarFile))
      .asInstanceOf[FormDataMultiPart]

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .post(Entity.entity(multipart, MediaType.MULTIPART_FORM_DATA))
    assert(response.getStatus === 200)
    val batch = response.readEntity(classOf[Batch])
    batchVersion match {
      case "1" =>
        assert(batch.getKyuubiInstance === fe.connectionUrl)
      case "2" if batch.getState === "INITIALIZED" =>
        assert(batch.getKyuubiInstance === null)
      case "2" if batch.getState === "PENDING" => // batch picked by BatchService
        assert(batch.getKyuubiInstance === fe.connectionUrl)
      case _ =>
        fail(s"unexpected batch info, version: $batchVersion state: ${batch.getState}")
    }
    assert(batch.getBatchType === "SPARK")
    assert(batch.getName === sparkBatchTestAppName)
    assert(batch.getCreateTime > 0)
    assert(batch.getEndTime === 0)

    // wait for batch be scheduled
    eventually(timeout(5.seconds), interval(200.millis)) {
      val resp = webTarget.path(s"api/v1/batches/${batch.getId}")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
        .get()
      val batchState = resp.readEntity(classOf[Batch]).getState
      assert(batchState === "PENDING" || batchState === "RUNNING")
    }

    webTarget.path(s"api/v1/batches/${batch.getId}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .delete()
    eventually(timeout(5.seconds), interval(200.millis)) {
      assert(KyuubiApplicationManager.uploadWorkDir.toFile.listFiles().isEmpty)
    }
  }

  test("open batch session w/ batch id") {
    val batchId = UUID.randomUUID().toString
    val reqObj = newSparkBatchRequest(Map(
      "spark.master" -> "local",
      KYUUBI_BATCH_ID_KEY -> batchId))

    val resp1 = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .post(Entity.entity(reqObj, MediaType.APPLICATION_JSON_TYPE))
    assert(resp1.getStatus === 200)
    val batch1 = resp1.readEntity(classOf[Batch])
    assert(batch1.getId === batchId)

    val resp2 = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .post(Entity.entity(reqObj, MediaType.APPLICATION_JSON_TYPE))
    assert(resp2.getStatus === 200)
    val batch2 = resp2.readEntity(classOf[Batch])
    assert(batch2.getId === batchId)

    assert(batch1.getCreateTime === batch2.getCreateTime)
    assert(BatchUtils.isDuplicatedSubmission(batch2))
  }

  test("get batch session list") {
    val sessionManager = server.frontendServices.head
      .be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach(_.close())

    val response = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("batchUser", "anonymous")
      .queryParam("batchState", "RUNNING")
      .queryParam("createTime", "0")
      .queryParam("endTime", "0")
      .queryParam("from", "0")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()

    assert(response.getStatus === 200)
    val getBatchListResponse = response.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse.getBatches.isEmpty && getBatchListResponse.getTotal === 0)

    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      newBatchRequest(
        "spark",
        sparkBatchTestResource.get,
        "",
        "",
        Map(KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString)))
    sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      "",
      "",
      "",
      Map.empty)
    sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      "",
      "",
      "",
      Map.empty)
    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      newBatchRequest(
        "spark",
        sparkBatchTestResource.get,
        "",
        "",
        Map(KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString)))
    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      newBatchRequest(
        "spark",
        sparkBatchTestResource.get,
        "",
        "",
        Map(KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString)))

    val response2 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "0")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()

    assert(response2.getStatus === 200)

    val getBatchListResponse2 = response2.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse2.getTotal === 2)

    val response3 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()

    assert(response3.getStatus === 200)

    val getBatchListResponse3 = response3.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse3.getTotal === 1)

    val response4 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "3")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()

    assert(response4.getStatus === 200)
    val getBatchListResponse4 = response4.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse4.getBatches.isEmpty && getBatchListResponse4.getTotal === 0)

    val response5 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "mock")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()

    assert(response5.getStatus === 200)

    val getBatchListResponse5 = response5.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse5.getTotal === 0)

    // TODO add more test when add more batchType
    val response6 = webTarget.path("api/v1/batches")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()

    assert(response6.getStatus === 200)
    val getBatchListResponse6 = response6.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse6.getTotal === 1)
    sessionManager.allSessions().foreach(_.close())

    val queryCreateTime = System.currentTimeMillis()
    val response7 = webTarget.path("api/v1/batches")
      .queryParam("createTime", queryCreateTime.toString)
      .queryParam("endTime", (queryCreateTime - 1).toString)
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    assert(response7.getStatus === 500)

    val response8 = webTarget.path("api/v1/batches")
      .queryParam("from", "0")
      .queryParam("size", "1")
      .queryParam("desc", "false")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    val firstBatch = response8.readEntity(classOf[GetBatchesResponse]).getBatches.get(0)

    val response9 = webTarget.path("api/v1/batches")
      .queryParam("from", "0")
      .queryParam("size", "1")
      .queryParam("desc", "true")
      .queryParam("createTime", "1")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    val lastBatch = response9.readEntity(classOf[GetBatchesResponse]).getBatches.get(0)
    assert(firstBatch.getCreateTime < lastBatch.getCreateTime)
  }

  test("negative request") {
    // open batch session
    Seq(
      (
        newBatchRequest(
          null,
          sparkBatchTestResource.get,
          sparkBatchTestMainClass,
          sparkBatchTestAppName),
        "is not in the supported list"),
      (
        newBatchRequest(
          "sp",
          sparkBatchTestResource.get,
          sparkBatchTestMainClass,
          sparkBatchTestAppName),
        "is not in the supported list"),
      (
        newBatchRequest(
          "SPARK",
          null,
          sparkBatchTestMainClass,
          "test-name"),
        "resource is a required parameter")).foreach { case (req, msg) =>
      val response = webTarget.path("api/v1/batches")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
        .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE))
      assert(response.getStatus === 500)
      assert(response.readEntity(classOf[String]).contains(msg))
    }

    // get batch by id
    Seq(
      ("??", "Invalid batchId: ??"),
      (
        "3ea7ddbe-0c35-45da-85ad-3186770181a7",
        "Invalid batchId: 3ea7ddbe-0c35-45da-85ad-3186770181a7")).foreach { case (batchId, msg) =>
      val response = webTarget.path(s"api/v1/batches/$batchId")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
        .get
      assert(response.getStatus === 404)
      assert(response.readEntity(classOf[String]).contains(msg))
    }
  }

  test("batch sessions recovery") {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val kyuubiInstance = fe.connectionUrl

    assert(sessionManager.getActiveUserSessionCount === 0)
    val batchId1 = UUID.randomUUID().toString
    val batchId2 = UUID.randomUUID().toString

    val batchMetadata = Metadata(
      identifier = batchId1,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "localhost",
      kyuubiInstance = kyuubiInstance,
      state = OperationState.PENDING.toString,
      resource = sparkBatchTestResource.get,
      className = sparkBatchTestMainClass,
      requestName = "PENDING_RECOVERY",
      requestConf = Map("spark.master" -> "local"),
      requestArgs = Seq.empty,
      createTime = System.currentTimeMillis(),
      engineType = "SPARK")

    val batchMetadata2 = batchMetadata.copy(
      identifier = batchId2,
      requestName = "RUNNING_RECOVERY")
    sessionManager.insertMetadata(batchMetadata)
    sessionManager.insertMetadata(batchMetadata2)

    assert(sessionManager.getBatchFromMetadataStore(batchId1).map(_.getState).contains("PENDING"))
    assert(sessionManager.getBatchFromMetadataStore(batchId2).map(_.getState).contains("PENDING"))

    val sparkBatchProcessBuilder = new SparkBatchProcessBuilder(
      "kyuubi",
      conf,
      batchId2,
      "RUNNING_RECOVERY",
      sparkBatchTestResource,
      sparkBatchTestMainClass,
      batchMetadata2.requestConf,
      batchMetadata2.requestArgs,
      None)
    sparkBatchProcessBuilder.start

    var applicationStatus: Option[ApplicationInfo] = None
    eventually(timeout(5.seconds)) {
      applicationStatus =
        sessionManager.applicationManager.getApplicationInfo(ApplicationManagerInfo(None), batchId2)
      assert(applicationStatus.isDefined)
    }

    val metadataToUpdate = Metadata(
      identifier = batchId2,
      state = OperationState.RUNNING.toString,
      engineId = applicationStatus.get.id,
      engineName = applicationStatus.get.name,
      engineUrl = applicationStatus.get.url.orNull,
      engineState = applicationStatus.get.state.toString,
      engineError = applicationStatus.get.error)
    sessionManager.updateMetadata(metadataToUpdate)

    val restFe = fe.asInstanceOf[KyuubiRestFrontendService]
    restFe.recoverBatchSessions()
    assert(sessionManager.getActiveUserSessionCount === 2)

    val sessionHandle1 = SessionHandle.fromUUID(batchId1)
    val sessionHandle2 = SessionHandle.fromUUID(batchId2)
    val session1 = sessionManager.getSession(sessionHandle1).asInstanceOf[KyuubiBatchSession]
    val session2 = sessionManager.getSession(sessionHandle2).asInstanceOf[KyuubiBatchSession]
    assert(session1.createTime === batchMetadata.createTime)
    assert(session2.createTime === batchMetadata2.createTime)

    eventually(timeout(10.seconds)) {
      assert(session1.batchJobSubmissionOp.getStatus.state === OperationState.RUNNING ||
        session1.batchJobSubmissionOp.getStatus.state === OperationState.FINISHED)
      assert(session1.batchJobSubmissionOp.builder.processLaunched)

      assert(session2.batchJobSubmissionOp.getStatus.state === OperationState.RUNNING ||
        session2.batchJobSubmissionOp.getStatus.state === OperationState.FINISHED)
      assert(!session2.batchJobSubmissionOp.builder.processLaunched)
    }

    assert(sessionManager.getBatchesFromMetadataStore(
      MetadataFilter(engineType = "SPARK"),
      0,
      Int.MaxValue).size === 2)
  }

  test("reassign batch") {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val kyuubiInstance = fe.connectionUrl

    assert(sessionManager.getActiveUserSessionCount === 0)
    val batchId1 = UUID.randomUUID().toString
    val batchId2 = UUID.randomUUID().toString

    val batchMetadata = Metadata(
      identifier = batchId1,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "localhost",
      kyuubiInstance = "other_kyuubi_instance:10099",
      state = OperationState.PENDING.toString,
      resource = sparkBatchTestResource.get,
      className = sparkBatchTestMainClass,
      requestName = "PENDING_RECOVERY",
      requestConf = Map("spark.master" -> "local"),
      requestArgs = Seq.empty,
      createTime = System.currentTimeMillis(),
      engineType = "SPARK")

    val batchMetadata2 = batchMetadata.copy(
      identifier = batchId2,
      requestName = "RUNNING_RECOVERY")
    sessionManager.insertMetadata(batchMetadata)
    sessionManager.insertMetadata(batchMetadata2)

    assert(sessionManager.getBatchFromMetadataStore(batchId1).map(_.getState).contains("PENDING"))
    assert(sessionManager.getBatchFromMetadataStore(batchId2).map(_.getState).contains("PENDING"))

    val sparkBatchProcessBuilder = new SparkBatchProcessBuilder(
      "kyuubi",
      conf,
      batchId2,
      "RUNNING_RECOVERY",
      sparkBatchTestResource,
      sparkBatchTestMainClass,
      batchMetadata2.requestConf,
      batchMetadata2.requestArgs,
      None)
    sparkBatchProcessBuilder.start

    var applicationStatus: Option[ApplicationInfo] = None
    eventually(timeout(5.seconds)) {
      applicationStatus =
        sessionManager.applicationManager.getApplicationInfo(ApplicationManagerInfo(None), batchId2)
      assert(applicationStatus.isDefined)
    }

    val metadataToUpdate = Metadata(
      identifier = batchId2,
      state = OperationState.RUNNING.toString,
      engineId = applicationStatus.get.id,
      engineName = applicationStatus.get.name,
      engineUrl = applicationStatus.get.url.orNull,
      engineState = applicationStatus.get.state.toString,
      engineError = applicationStatus.get.error)
    sessionManager.updateMetadata(metadataToUpdate)

    val requestObj = new ReassignBatchRequest("other_kyuubi_instance:10099")
    val response = webTarget.path("api/v1/batches/reassign")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 200)
    val batch = response.readEntity(classOf[ReassignBatchResponse])
    assert(batch.getBatchIds.size() === 2)
    assert(batch.getBatchIds.contains(batchId1))
    assert(batch.getBatchIds.contains(batchId2))
    assert(sessionManager.getBatchMetadata(batchId1).map(_.kyuubiInstance).contains(kyuubiInstance))
    assert(sessionManager.getBatchMetadata(batchId2).map(_.kyuubiInstance).contains(kyuubiInstance))
    assert(sessionManager.getActiveUserSessionCount === 2)

    val sessionHandle1 = SessionHandle.fromUUID(batchId1)
    val sessionHandle2 = SessionHandle.fromUUID(batchId2)
    val session1 = sessionManager.getSession(sessionHandle1).asInstanceOf[KyuubiBatchSession]
    val session2 = sessionManager.getSession(sessionHandle2).asInstanceOf[KyuubiBatchSession]
    assert(session1.createTime === batchMetadata.createTime)
    assert(session2.createTime === batchMetadata2.createTime)

    eventually(timeout(10.seconds)) {
      assert(session1.batchJobSubmissionOp.getStatus.state === OperationState.RUNNING ||
        session1.batchJobSubmissionOp.getStatus.state === OperationState.FINISHED)
      assert(session1.batchJobSubmissionOp.builder.processLaunched)

      assert(session2.batchJobSubmissionOp.getStatus.state === OperationState.RUNNING ||
        session2.batchJobSubmissionOp.getStatus.state === OperationState.FINISHED)
      assert(!session2.batchJobSubmissionOp.builder.processLaunched)
    }

    assert(sessionManager.getBatchesFromMetadataStore(
      MetadataFilter(engineType = "SPARK"),
      0,
      Int.MaxValue).size === 2)
  }

  test("get local log internal redirection") {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val metadata = Metadata(
      identifier = UUID.randomUUID().toString,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "localhost",
      kyuubiInstance = fe.connectionUrl,
      state = "PENDING",
      resource = "resource",
      className = "className",
      requestName = "LOCAL_LOG_NOT_FOUND",
      engineType = "SPARK")
    sessionManager.insertMetadata(metadata)

    // get local batch log in the same kyuubi instance
    var logResponse = webTarget.path(s"api/v1/batches/${metadata.identifier}/localLog")
      .queryParam("from", "0")
      .queryParam("size", "1")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    batchVersion match {
      case "1" =>
        assert(logResponse.getStatus === 404)
        assert(logResponse.readEntity(classOf[String]).contains("No local log found"))
      case "2" =>
        assert(logResponse.getStatus === 200)
        assert(logResponse.readEntity(classOf[String]).contains(
          s"Batch ${metadata.identifier} is waiting for submitting"))
      case _ =>
        fail(s"unexpected batch version: $batchVersion")
    }

    // get local batch log that is not existing
    logResponse = webTarget.path(s"api/v1/batches/${UUID.randomUUID.toString}/localLog")
      .queryParam("from", "0")
      .queryParam("size", "1")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    assert(logResponse.getStatus === 404)
    assert(logResponse.readEntity(classOf[String]).contains("Invalid batchId"))

    val metadata2 = metadata.copy(
      identifier = UUID.randomUUID().toString,
      kyuubiInstance = "other_kyuubi_instance:10099")
    sessionManager.insertMetadata(metadata2)

    // get local batch log that need make redirection
    logResponse = webTarget.path(s"api/v1/batches/${metadata2.identifier}/localLog")
      .queryParam("from", "0")
      .queryParam("size", "1")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()
    assert(logResponse.getStatus === 500)
    assert(logResponse.readEntity(classOf[String]).contains(
      s"Api request failed for http://${metadata2.kyuubiInstance}"))
  }

  test("delete batch internal redirection") {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val metadata = Metadata(
      identifier = UUID.randomUUID().toString,
      sessionType = SessionType.BATCH,
      realUser = "kyuubi",
      username = "kyuubi",
      ipAddress = "localhost",
      kyuubiInstance = fe.connectionUrl,
      state = "PENDING",
      resource = "resource",
      className = "className",
      requestName = "LOCAL_LOG_NOT_FOUND",
      engineType = "SPARK")
    sessionManager.insertMetadata(metadata)

    // delete the batch in the same kyuubi instance but not found in-memory
    var deleteResp = webTarget.path(s"api/v1/batches/${metadata.identifier}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("kyuubi"))
      .delete()
    assert(deleteResp.getStatus === 200)
    assert(!deleteResp.readEntity(classOf[CloseBatchResponse]).isSuccess)

    // delete batch that is not existing
    deleteResp = webTarget.path(s"api/v1/batches/${UUID.randomUUID.toString}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("kyuubi"))
      .delete()
    assert(deleteResp.getStatus === 404)
    assert(deleteResp.readEntity(classOf[String]).contains("Invalid batchId:"))

    val metadata2 = metadata.copy(
      identifier = UUID.randomUUID().toString,
      kyuubiInstance = "other_kyuubi_instance:10099")
    sessionManager.insertMetadata(metadata2)

    // delete batch that need make redirection
    deleteResp = webTarget.path(s"api/v1/batches/${metadata2.identifier}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("kyuubi"))
      .delete()
    assert(deleteResp.getStatus === 200)
    assert(deleteResp.readEntity(classOf[CloseBatchResponse]).getMsg.contains(
      s"Api request failed for http://${metadata2.kyuubiInstance}"))
  }

  test("support to get the real client ip for http proxy use case") {
    val realClientIp = "localtest.me"

    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .header(conf.get(FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER), realClientIp)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 200)
    val batch = response.readEntity(classOf[Batch])
    eventually(timeout(10.seconds)) {
      val batchSession = sessionManager.getBatchSession(SessionHandle.fromUUID(batch.getId))
      assert(batchSession.map(_.ipAddress).contains(realClientIp))
    }
  }

  test("expose the metrics with operation type and current state") {
    eventually(timeout(10.seconds)) {
      assert(getBatchJobSubmissionStateCounter(OperationState.INITIALIZED) === 0)
      assert(getBatchJobSubmissionStateCounter(OperationState.PENDING) === 0)
      assert(getBatchJobSubmissionStateCounter(OperationState.RUNNING) === 0)
    }

    val originalTerminatedCount =
      getBatchJobSubmissionStateCounter(OperationState.CANCELED) +
        getBatchJobSubmissionStateCounter(OperationState.FINISHED) +
        getBatchJobSubmissionStateCounter(OperationState.ERROR)

    val batchId = UUID.randomUUID().toString
    val requestObj = newBatchRequest(
      sparkBatchTestBatchType,
      sparkBatchTestResource.get,
      "org.apache.spark.examples.DriverSubmissionTest",
      "DriverSubmissionTest-" + batchId,
      Map(
        "spark.master" -> "local",
        KYUUBI_BATCH_ID_KEY -> batchId),
      Seq("120"))

    eventually(timeout(10.seconds)) {
      val response = webTarget.path("api/v1/batches")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
      assert(response.getStatus === 200)
      val batch = response.readEntity(classOf[Batch])
      assert(batch.getState === OperationState.PENDING.toString ||
        batch.getState === OperationState.RUNNING.toString)
    }

    eventually(timeout(20.seconds)) {
      assert(getBatchJobSubmissionStateCounter(OperationState.RUNNING) === 1)
    }

    val deleteResp = webTarget.path(s"api/v1/batches/$batchId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .delete()
    assert(deleteResp.getStatus === 200)

    eventually(timeout(10.seconds)) {
      assert(getBatchJobSubmissionStateCounter(OperationState.INITIALIZED) === 0)
      assert(getBatchJobSubmissionStateCounter(OperationState.PENDING) === 0)
      assert(getBatchJobSubmissionStateCounter(OperationState.RUNNING) === 0)
    }

    val currentTerminatedCount = getBatchJobSubmissionStateCounter(OperationState.CANCELED) +
      getBatchJobSubmissionStateCounter(OperationState.FINISHED) +
      getBatchJobSubmissionStateCounter(OperationState.ERROR)
    assert(currentTerminatedCount - originalTerminatedCount === 1)
  }

  private def getBatchJobSubmissionStateCounter(state: OperationState): Long = {
    val opType = classOf[BatchJobSubmission].getSimpleName
    val counterName = s"${MetricsConstants.OPERATION_STATE}.$opType.${state.toString.toLowerCase}"
    MetricsSystem.meterValue(counterName).getOrElse(0L)
  }

  test("the batch session should be consistent on open session failure") {
    val sessionManager = server.frontendServices.head
      .be.sessionManager.asInstanceOf[KyuubiSessionManager]

    val e = intercept[Exception] {
      val conf = Map(
        KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString,
        "spark.jars" -> "disAllowPath")
      sessionManager.openBatchSession(
        "kyuubi",
        "kyuubi",
        InetAddress.getLocalHost.getCanonicalHostName,
        newSparkBatchRequest(conf))
    }
    val sessionHandleRegex = "\\[\\S*]".r
    val batchId = sessionHandleRegex.findFirstMatchIn(e.getMessage).get.group(0)
      .replaceAll("\\[", "").replaceAll("]", "")
    assert(sessionManager.getBatchMetadata(batchId).map(_.state).contains("CANCELED"))
  }

  test("get batch list with batch name filter condition") {
    val sessionManager = server.frontendServices.head
      .be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach(_.close())

    val uniqueName = UUID.randomUUID().toString
    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      newBatchRequest(
        "spark",
        sparkBatchTestResource.get,
        "",
        uniqueName,
        Map(KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString)))

    val response = webTarget.path("api/v1/batches")
      .queryParam("batchName", uniqueName)
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
      .get()

    assert(response.getStatus == 200)
    val getBatchListResponse = response.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse.getTotal == 1)
  }

  test("open batch session with proxyUser") {
    val normalUser = "kyuubi"

    def runOpenBatchExecutor(
        kyuubiProxyUser: Option[String],
        hs2ProxyUser: Option[String]): Response = {
      val conf = mutable.Map("spark.master" -> "local")

      kyuubiProxyUser.map { username =>
        conf += (PROXY_USER.key -> username)
      }
      hs2ProxyUser.map { username =>
        conf += (AuthUtils.HS2_PROXY_USER -> username)
      }
      val proxyUserRequest = newSparkBatchRequest(conf.toMap)

      webTarget.path("api/v1/batches")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("anonymous"))
        .post(Entity.entity(proxyUserRequest, MediaType.APPLICATION_JSON_TYPE))
    }

    // use kyuubi.session.proxy.user
    val proxyUserResponse1 = runOpenBatchExecutor(Option(normalUser), None)
    assert(proxyUserResponse1.getStatus === 403)
    val errorMessage = s"Failed to validate proxy privilege of anonymous for $normalUser"
    assert(proxyUserResponse1.readEntity(classOf[String]).contains(errorMessage))

    // it should be the same behavior as hive.server2.proxy.user
    val proxyUserResponse2 = runOpenBatchExecutor(None, Option(normalUser))
    assert(proxyUserResponse2.getStatus === 403)
    assert(proxyUserResponse2.readEntity(classOf[String]).contains(errorMessage))

    // when both set, kyuubi.session.proxy.user takes precedence
    val proxyUserResponse3 =
      runOpenBatchExecutor(Option(normalUser), Option(s"${normalUser}HiveServer2"))
    assert(proxyUserResponse3.getStatus === 403)
    assert(proxyUserResponse3.readEntity(classOf[String]).contains(errorMessage))
  }
}
