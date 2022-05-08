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

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CHECK_INTERVAL, ENGINE_SPARK_MAX_LIFETIME}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.server.http.authentication.AuthenticationHandler.AUTHORIZATION_HEADER
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.KyuubiSessionManager

class BatchesResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {
  test("open batch session") {
    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)
    val requestObj = BatchRequest(
      "spark",
      sparkProcessBuilder.mainResource.get,
      sparkProcessBuilder.mainClass,
      "spark-batch-submission",
      Map(
        "spark.master" -> "local",
        s"spark.${ENGINE_SPARK_MAX_LIFETIME.key}" -> "5000",
        s"spark.${ENGINE_CHECK_INTERVAL.key}" -> "1000"),
      Seq.empty[String])

    val response = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    var batch = response.readEntity(classOf[Batch])
    assert(batch.kyuubiInstance === fe.connectionUrl)
    assert(batch.batchType === "spark")

    val proxyUserRequest = requestObj.copy(conf = requestObj.conf ++
      Map(KyuubiAuthenticationFactory.HS2_PROXY_USER -> "root"))
    val proxyUserResponse = webTarget.path("api/v1/batches")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(proxyUserRequest, MediaType.APPLICATION_JSON_TYPE))
    assert(500 == proxyUserResponse.getStatus)

    var getBatchResponse = webTarget.path(s"api/v1/batches/${batch.id}")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(200 == getBatchResponse.getStatus)
    batch = getBatchResponse.readEntity(classOf[Batch])
    assert(batch.kyuubiInstance === fe.connectionUrl)
    assert(batch.batchType === "spark")

    // invalid batchId
    getBatchResponse = webTarget.path(s"api/v1/batches/invalidBatchId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()
    assert(404 == getBatchResponse.getStatus)

    // invalid user name
    val encodeAuthorization = new String(Base64.getEncoder.encode(batch.id.getBytes()), "UTF-8")
    var deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.id}")
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
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.id}")
      .queryParam("hive.server2.proxy.user", "invalidProxy")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(405 == deleteBatchResponse.getStatus)

    // killApp is true
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.id}")
      .queryParam("killApp", "true")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .delete()
    assert(200 == deleteBatchResponse.getStatus)
    assert(deleteBatchResponse.hasEntity)

    // close the closed batch session
    deleteBatchResponse = webTarget.path(s"api/v1/batches/${batch.id}")
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
    assert(getBatchListResponse.batches.isEmpty && getBatchListResponse.total == 0)

    sessionManager.openBatchSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2,
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      BatchRequest("spark", "", "", "", Map.empty, Seq.empty))
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
      BatchRequest("spark", "", "", "", Map.empty, Seq.empty))
    sessionManager.openBatchSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2,
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      BatchRequest("spark", "", "", "", Map.empty, Seq.empty))

    val response2 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "0")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response2.getStatus == 200)

    val getBatchListResponse2 = response2.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse2.total == 2)

    val response3 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response3.getStatus == 200)

    val getBatchListResponse3 = response3.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse3.total == 1)

    val response4 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "spark")
      .queryParam("from", "3")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response4.getStatus == 200)
    val getBatchListResponse4 = response4.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse4.batches.isEmpty && getBatchListResponse4.total == 0)

    val response5 = webTarget.path("api/v1/batches")
      .queryParam("batchType", "mock")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response5.getStatus == 200)

    val getBatchListResponse5 = response5.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse5.total == 0)

    // TODO add more test when add more batchType
    val response6 = webTarget.path("api/v1/batches")
      .queryParam("from", "2")
      .queryParam("size", "2")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .get()

    assert(response6.getStatus == 200)
    val getBatchListResponse6 = response6.readEntity(classOf[GetBatchesResponse])
    assert(getBatchListResponse6.total == 1)
    sessionManager.allSessions().map(_.close())
  }
}
