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

import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CHECK_INTERVAL, ENGINE_SPARK_MAX_LIFETIME}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory

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
  }
}
