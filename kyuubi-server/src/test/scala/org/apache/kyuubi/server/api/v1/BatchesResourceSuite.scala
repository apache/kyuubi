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
import java.util.Base64
import java.util.UUID
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CHECK_INTERVAL, ENGINE_SPARK_MAX_LIFETIME}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.server.http.authentication.AuthenticationHandler.AUTHORIZATION_HEADER
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager}

class BatchesResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  override def afterEach(): Unit = {
    val sessionManager = fe.be.sessionManager
    sessionManager.asInstanceOf[KyuubiSessionManager]
      .getBatchSessionList(null, 0, Int.MaxValue)
      .map(_.asInstanceOf[KyuubiBatchSessionImpl])
      .foreach { session =>
        try {
          session.submitBatchAppOp.killBatchApplication()
        } finally {
          sessionManager.closeSession(session.handle)
        }
      }
  }

  test("open batch session") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)
    val appName = "spark-batch-submission"
    val requestObj = new BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      sparkProcessBuilder.mainClass,
      appName,
      Map(
        "spark.master" -> "local",
        s"spark.${ENGINE_SPARK_MAX_LIFETIME.key}" -> "5000",
        s"spark.${ENGINE_CHECK_INTERVAL.key}" -> "1000").asJava,
      Seq.empty[String].asJava)

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    var batch = response.readEntity(classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "spark")

    requestObj.setConf((requestObj.getConf.asScala ++
      Map(KyuubiAuthenticationFactory.HS2_PROXY_USER -> "root")).asJava)
    val proxyUserRequest = requestObj
    val proxyUserResponse = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(proxyUserRequest, MediaType.APPLICATION_JSON_TYPE))
    assert(500 == proxyUserResponse.getStatus)

    var getBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(200 == getBatchResponse.getStatus)
    batch = getBatchResponse.readEntity(classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "spark")

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
        .queryParam("from", "0")
        .queryParam("size", "100")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .get()
      log = logResponse.readEntity(classOf[OperationLog])
      if (log.getRowCount > 0) {
        log.getLogRowSet.asScala.foreach(logs.append(_))
      }

      // check both kyuubi log and engine log
      assert(logs.exists(_.contains("/bin/spark-submit")) && logs.exists(
        _.contains(s"SparkContext: Submitted application: $appName")))
    }

    // invalid user name
    val encodeAuthorization =
      new String(Base64.getEncoder.encode(batch.getId().getBytes()), "UTF-8")
    var deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .queryParam("killApp", "true")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .delete()
    assert(405 == deleteBatchResponse.getStatus)

    // invalid batchId
    deleteBatchResponse = webTarget.path(s"api/v1/batches/notValidUUID")
      .queryParam("killApp", "true")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(404 == deleteBatchResponse.getStatus)

    // non-existed batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${UUID.randomUUID().toString}")
      .queryParam("killApp", "true")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(404 == deleteBatchResponse.getStatus)

    // invalid proxy user
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .queryParam("hive.server2.proxy.user", "invalidProxy")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(405 == deleteBatchResponse.getStatus)

    // killApp is true
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .queryParam("killApp", "true")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(200 == deleteBatchResponse.getStatus)
    assert(deleteBatchResponse.hasEntity)

    // close the closed batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.getId()}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(404 == deleteBatchResponse.getStatus)
  }

  test("get batch session list") {
    val sessionManager = server.frontendServices.head
      .be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach(_.close())

    val response = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "0")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response.getStatus == 200)
    val getBatchListResponse = response.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse.getBatches.isEmpty && getBatchListResponse.getTotal == 0)

    sessionManager.openBatchSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2,
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      new BatchRequest(
        "spark",
        "",
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
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2,
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      new BatchRequest(
        "spark",
        "",
        "",
        ""))
    sessionManager.openBatchSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2,
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      new BatchRequest(
        "spark",
        "",
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
  }

  test("negative request") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)

    // open batch session
    Seq(
      (
        new BatchRequest(
          null,
          sparkProcessBuilder.mainResource.get,
          sparkProcessBuilder.mainClass,
          "test-name"),
        "batchType is a required parameter"),
      (
        new BatchRequest(
          "sp",
          sparkProcessBuilder.mainResource.get,
          sparkProcessBuilder.mainClass,
          "test-name"),
        "due to Batch type sp unsupported"),
      (
        new BatchRequest(
          "SPARK",
          null,
          sparkProcessBuilder.mainClass,
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
}
