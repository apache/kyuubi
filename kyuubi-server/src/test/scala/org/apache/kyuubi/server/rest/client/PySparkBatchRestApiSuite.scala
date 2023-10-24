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

import java.nio.file.Paths

import org.apache.kyuubi.{BatchTestHelper, RestClientTestHelper}
import org.apache.kyuubi.client.{BatchRestApi, KyuubiRestClient}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkProcessBuilder

class PySparkBatchRestApiSuite extends RestClientTestHelper with BatchTestHelper {
  object PySparkJobPI {
    val batchType = "PYSPARK"
    val className: String = null // For PySpark, mainClass isn't needed.
    val name = "PythonPi" // the app name is hard coded in spark example code
    lazy val resource: Option[String] = {
      val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", KyuubiConf())
      Paths.get(
        sparkProcessBuilder.sparkHome,
        "examples",
        "src",
        "main",
        "python").toFile.listFiles().find(
        _.getName.equalsIgnoreCase("pi.py")) map (_.getCanonicalPath)
    }
  }

  test("pyspark submit - basic batch rest client with existing resource file") {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val requestObj = newBatchRequest(
      batchType = PySparkJobPI.batchType,
      resource = PySparkJobPI.resource.get,
      className = null,
      name = PySparkJobPI.name,
      conf = Map("spark.master" -> "local"),
      args = Seq("10"))
    val batch: Batch = batchRestApi.createBatch(requestObj)

    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "PYSPARK")
    basicKyuubiRestClient.close()
  }

  test("pyspark submit - basic batch rest client with uploading resource file") {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()
    val batchRestApi: BatchRestApi = new BatchRestApi(basicKyuubiRestClient)

    val requestObj = newBatchRequest(
      batchType = PySparkJobPI.batchType,
      resource = null,
      className = null,
      name = PySparkJobPI.name,
      conf = Map("spark.master" -> "local"),
      args = Seq("10"))
    val resourceFile = Paths.get(PySparkJobPI.resource.get).toFile
    val batch: Batch = batchRestApi.createBatch(requestObj, resourceFile)

    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "PYSPARK")
    basicKyuubiRestClient.close()
  }
}
