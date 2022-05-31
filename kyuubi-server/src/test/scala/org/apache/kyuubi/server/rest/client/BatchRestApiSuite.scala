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

package org.apache.kyuubi.server.rest.client

import scala.collection.JavaConverters._

import org.apache.kyuubi.RestClientTestHelper
import org.apache.kyuubi.client.{BatchRestApi, KyuubiRestClient}
import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CHECK_INTERVAL, ENGINE_SPARK_MAX_LIFETIME}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder

class BatchRestApiSuite extends RestClientTestHelper {

  test("basic batch rest client") {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authSchema(KyuubiRestClient.AuthSchema.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    // create batch
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

    var batch: Batch = batchRestApi.createBatch(requestObj)
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "spark")

    // get batch by id
    batch = batchRestApi.getBatchById(batch.getId())
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "spark")

    // get batch log
    var log = batchRestApi.getBatchLocalLog(batch.getId(), 0, 1)
    assert(log.getRowCount == 1)

    // delete batch
    batchRestApi.deleteBatch(batch.getId(), true, null)

    basicKyuubiRestClient.close()
  }

  test("basic batch rest client with invalid user") {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authSchema(KyuubiRestClient.AuthSchema.BASIC)
        .username(customUser)
        .password(customPasswd)
        .socketTimeout(30000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val e = intercept[KyuubiRestException] {
      // get batch by id
      batchRestApi.getBatchById("1")
    }
    assert(e.getCause.toString.contains(s"Error validating LDAP user: uid=${customUser}"))

    basicKyuubiRestClient.close()
  }

  test("spnego batch rest client") {
    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authSchema(KyuubiRestClient.AuthSchema.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(spnegoKyuubiRestClient)
    // create batch
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

    val batch: Batch = batchRestApi.createBatch(requestObj)
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "spark")

    spnegoKyuubiRestClient.close()
  }
}
