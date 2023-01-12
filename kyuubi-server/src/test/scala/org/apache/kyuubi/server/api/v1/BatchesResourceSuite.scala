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
import java.util.{Base64, UUID}
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{BatchTestHelper, KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.ApplicationInfo
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.{BatchJobSubmission, OperationState}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.http.authentication.AuthenticationHandler.AUTHORIZATION_HEADER
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.service.authentication.{KyuubiAuthenticationFactory, UserDefinedEngineSecuritySecretProvider}
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager, SessionHandle, SessionType}

class BatchesResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper with BatchTestHelper {
  override protected lazy val conf: KyuubiConf = KyuubiConf()
    .set(KyuubiConf.ENGINE_SECURITY_ENABLED, true)
    .set(
      KyuubiConf.ENGINE_SECURITY_SECRET_PROVIDER,
      classOf[UserDefinedEngineSecuritySecretProvider].getName)
    .set(
      KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST,
      Seq(Paths.get(sparkBatchTestResource.get).getParent.toString))

  override def afterEach(): Unit = {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach { session =>
      sessionManager.closeSession(session.handle)
    }
    sessionManager.getBatchesFromMetadataStore(null, null, null, 0, 0, 0, Int.MaxValue).foreach {
      batch =>
        sessionManager.applicationManager.killApplication(None, batch.getId)
        sessionManager.cleanupMetadata(batch.getId)
    }
  }

  test("open batch session") {
    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    var batch = response.readEntity(classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")
    assert(batch.getName === sparkBatchTestAppName)
    assert(batch.getCreateTime > 0)
    assert(batch.getEndTime === 0)

    requestObj.setConf((requestObj.getConf.asScala ++
      Map(KyuubiAuthenticationFactory.HS2_PROXY_USER -> "root")).asJava)
    val proxyUserRequest = requestObj
    val proxyUserResponse = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(proxyUserRequest, MediaType.APPLICATION_JSON_TYPE))
    assert(405 == proxyUserResponse.getStatus)
    var errorMessage = "Failed to validate proxy privilege of anonymous for root"
    assert(proxyUserResponse.readEntity(classOf[String]).contains(errorMessage))

    var getBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(200 == getBatchResponse.getStatus)
    batch = getBatchResponse.readEntity(classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")
    assert(batch.getName === sparkBatchTestAppName)
    assert(batch.getCreateTime > 0)
    assert(batch.getEndTime === 0)

    // invalid batchId
    getBatchResponse = webTarget.path(s"api/v1/batches/invalidBatchId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(404 == getBatchResponse.getStatus)

    // get batch log
    var logResponse = webTarget.path(s"api/v1/batches/${batch.getId()}/localLog")
      .queryParam("from", "0")
      .queryParam("size", "1")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    var log = logResponse.readEntity(classOf[OperationLog])
    val head = log.getLogRowSet.asScala.head
    assert(log.getRowCount == 1)

    val logs = new ArrayBuffer[String]
    logs.append(head)
    eventually(timeout(10.seconds), interval(1.seconds)) {
      logResponse = webTarget.path(s"api/v1/batches/${batch.getId()}/localLog")
        .queryParam("from", "-1")
        .queryParam("size", "100")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get()
      log = logResponse.readEntity(classOf[OperationLog])
      if (log.getRowCount > 0) {
        log.getLogRowSet.asScala.foreach(logs.append(_))
      }

      // check both kyuubi log and engine log
      assert(
        logs.exists(_.contains("/bin/spark-submit")) && logs.exists(
          _.contains(s"SparkContext: Submitted application: $sparkBatchTestAppName")))
    }

    // invalid user name
    val encodeAuthorization =
      new String(Base64.getEncoder.encode(batch.getId().getBytes()), "UTF-8")
    var deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .delete()
    assert(405 == deleteBatchResponse.getStatus)
    errorMessage = s"${batch.getId()} is not allowed to close the session belong to anonymous"
    assert(deleteBatchResponse.readEntity(classOf[String]).contains(errorMessage))

    // invalid batchId
    deleteBatchResponse = webTarget.path(s"api/v1/batches/notValidUUID")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(404 == deleteBatchResponse.getStatus)

    // non-existed batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${UUID.randomUUID().toString}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(404 == deleteBatchResponse.getStatus)

    // invalid proxy user
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .queryParam("hive.server2.proxy.user", "invalidProxy")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(405 == deleteBatchResponse.getStatus)
    errorMessage = "Failed to validate proxy privilege of anonymous for invalidProxy"
    assert(deleteBatchResponse.readEntity(classOf[String]).contains(errorMessage))

    // check close batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(200 == deleteBatchResponse.getStatus)
    val closeBatchResponse = deleteBatchResponse.readEntity(classOf[CloseBatchResponse])

    // check state after close batch session
    getBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(200 == getBatchResponse.getStatus)
    batch = getBatchResponse.readEntity(classOf[Batch])
    assert(batch.getId == batch.getId())
    if (closeBatchResponse.isSuccess) {
      assert(batch.getState == "CANCELED")
    } else {
      assert(batch.getState != "CANCELED")
    }

    // close the closed batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(200 == deleteBatchResponse.getStatus)
    assert(!deleteBatchResponse.readEntity(classOf[CloseBatchResponse]).isSuccess)
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
      .get()

    assert(response.getStatus == 200)
    val getBatchListResponse = response.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse.getBatches.isEmpty && getBatchListResponse.getTotal == 0)

    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      newBatchRequest(
        "spark",
        sparkBatchTestResource.get,
        "",
        ""))
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
      Map.empty,
      newBatchRequest(
        "spark",
        sparkBatchTestResource.get,
        "",
        ""))
    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      newBatchRequest(
        "spark",
        sparkBatchTestResource.get,
        "",
        ""))

    val response2 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "0")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response2.getStatus == 200)

    val getBatchListResponse2 = response2.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse2.getTotal == 2)

    val response3 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response3.getStatus == 200)

    val getBatchListResponse3 = response3.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse3.getTotal == 1)

    val response4 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "3")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response4.getStatus == 200)
    val getBatchListResponse4 = response4.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse4.getBatches.isEmpty && getBatchListResponse4.getTotal == 0)

    val response5 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "mock")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response5.getStatus == 200)

    val getBatchListResponse5 = response5.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse5.getTotal == 0)

    // TODO add more test when add more batchType
    val response6 = webTarget.path("api/v1/batches")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response6.getStatus == 200)
    val getBatchListResponse6 = response6.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse6.getTotal == 1)
    sessionManager.allSessions().map(_.close())

    val queryCreateTime = System.currentTimeMillis()
    val response7 = webTarget.path("api/v1/batches")
      .queryParam("createTime", queryCreateTime.toString)
      .queryParam("endTime", (queryCreateTime - 1).toString)
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(response7.getStatus === 500)
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
        .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE))
      assert(500 == response.getStatus)
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
        .get
      assert(404 == response.getStatus)
      assert(response.readEntity(classOf[String]).contains(msg))
    }
  }

  test("batch sessions recovery") {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val kyuubiInstance = fe.connectionUrl

    assert(sessionManager.getOpenSessionCount == 0)
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

    assert(sessionManager.getBatchFromMetadataStore(batchId1).getState.equals("PENDING"))
    assert(sessionManager.getBatchFromMetadataStore(batchId2).getState.equals("PENDING"))

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
      applicationStatus = sessionManager.applicationManager.getApplicationInfo(None, batchId2)
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
    assert(sessionManager.getOpenSessionCount == 2)

    val sessionHandle1 = SessionHandle.fromUUID(batchId1)
    val sessionHandle2 = SessionHandle.fromUUID(batchId2)
    val session1 = sessionManager.getSession(sessionHandle1).asInstanceOf[KyuubiBatchSessionImpl]
    val session2 = sessionManager.getSession(sessionHandle2).asInstanceOf[KyuubiBatchSessionImpl]
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
      "SPARK",
      null,
      null,
      0,
      0,
      0,
      Int.MaxValue).size == 2)
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
      .get()
    assert(logResponse.getStatus == 404)
    assert(logResponse.readEntity(classOf[String]).contains("No local log found"))

    // get local batch log that is not existing
    logResponse = webTarget.path(s"api/v1/batches/${UUID.randomUUID.toString}/localLog")
      .queryParam("from", "0")
      .queryParam("size", "1")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(logResponse.getStatus == 404)
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
      .get()
    assert(logResponse.getStatus == 500)
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

    val encodeAuthorization =
      new String(Base64.getEncoder.encode("kyuubi".getBytes()), "UTF-8")

    // delete the batch in the same kyuubi instance but not found in-memory
    var deleteResp = webTarget.path(s"api/v1/batches/${metadata.identifier}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .delete()
    assert(deleteResp.getStatus == 200)
    assert(!deleteResp.readEntity(classOf[CloseBatchResponse]).isSuccess)

    // delete batch that is not existing
    deleteResp = webTarget.path(s"api/v1/batches/${UUID.randomUUID.toString}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .delete()
    assert(deleteResp.getStatus == 404)
    assert(deleteResp.readEntity(classOf[String]).contains("Invalid batchId:"))

    val metadata2 = metadata.copy(
      identifier = UUID.randomUUID().toString,
      kyuubiInstance = "other_kyuubi_instance:10099")
    sessionManager.insertMetadata(metadata2)

    // delete batch that need make redirection
    deleteResp = webTarget.path(s"api/v1/batches/${metadata2.identifier}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .delete()
    assert(deleteResp.getStatus == 200)
    assert(deleteResp.readEntity(classOf[CloseBatchResponse]).getMsg.contains(
      s"Api request failed for http://${metadata2.kyuubiInstance}"))
  }

  test("support to get the real client ip for http proxy use case") {
    val realClientIp = "localtest.me"

    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(conf.get(FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER), realClientIp)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    val batch = response.readEntity(classOf[Batch])
    val batchSession = sessionManager.getBatchSessionImpl(SessionHandle.fromUUID(batch.getId))
    assert(batchSession.ipAddress === realClientIp)
  }

  test("expose the metrics with operation type and current state") {
    eventually(timeout(10.seconds)) {
      assert(getBatchJobSubmissionStateCounter(OperationState.INITIALIZED) === 0)
      assert(getBatchJobSubmissionStateCounter(OperationState.PENDING) === 0)
      assert(getBatchJobSubmissionStateCounter(OperationState.RUNNING) === 0)
    }

    val originalTerminateCounter = getBatchJobSubmissionStateCounter(OperationState.CANCELED) +
      getBatchJobSubmissionStateCounter(OperationState.FINISHED) +
      getBatchJobSubmissionStateCounter(OperationState.ERROR)

    val requestObj = newSparkBatchRequest(Map("spark.master" -> "local"))

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    var batch = response.readEntity(classOf[Batch])

    assert(getBatchJobSubmissionStateCounter(OperationState.INITIALIZED) +
      getBatchJobSubmissionStateCounter(OperationState.PENDING) +
      getBatchJobSubmissionStateCounter(OperationState.RUNNING) === 1)

    while (batch.getState == OperationState.PENDING.toString ||
      batch.getState == OperationState.RUNNING.toString) {
      val deleteResp = webTarget.path(s"api/v1/batches/${batch.getId}")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .delete()
      assert(200 == deleteResp.getStatus)

      batch = webTarget.path(s"api/v1/batches/${batch.getId}")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get().readEntity(classOf[Batch])
    }

    assert(getBatchJobSubmissionStateCounter(OperationState.INITIALIZED) === 0)
    assert(getBatchJobSubmissionStateCounter(OperationState.PENDING) === 0)
    assert(getBatchJobSubmissionStateCounter(OperationState.RUNNING) === 0)

    val currentTeminateCount = getBatchJobSubmissionStateCounter(OperationState.CANCELED) +
      getBatchJobSubmissionStateCounter(OperationState.FINISHED) +
      getBatchJobSubmissionStateCounter(OperationState.ERROR)
    assert(currentTeminateCount - originalTerminateCounter === 1)
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
      sessionManager.openBatchSession(
        "kyuubi",
        "kyuubi",
        InetAddress.getLocalHost.getCanonicalHostName,
        Map.empty,
        newSparkBatchRequest(Map("spark.jars" -> "disAllowPath")))
    }
    val sessionHandleRegex = "\\[[\\S]*\\]".r
    val batchId = sessionHandleRegex.findFirstMatchIn(e.getMessage).get.group(0)
      .replaceAll("\\[", "").replaceAll("\\]", "")
    assert(sessionManager.getBatchMetadata(batchId).state == "CANCELED")
  }
}
