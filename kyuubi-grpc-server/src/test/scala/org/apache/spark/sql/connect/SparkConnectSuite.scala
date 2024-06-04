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

package org.apache.spark.sql.connect

import org.apache.spark.connect.proto.ConfigRequest
import org.apache.spark.sql.connect.client.SparkConnectClient

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class SparkConnectSuite extends KyuubiFunSuite {
  val server: TestServer = new TestServer("testServer")
  var client: SparkConnectClient = _
  override def beforeAll(): Unit = {
    val conf = KyuubiConf().set(KyuubiConf.ENGINE_SPARK_CONNECT_GRPC_BINDING_PORT, 10023)
    server.initialize(conf)
    server.start()
    client = SparkConnectClient.builder().port(server.frontendInfo()._2).build()
    super.beforeAll()

  }

  test("test config") {
    val request = ConfigRequest.newBuilder()
      .setSessionId("abc123")
      .build()

    val response = client.config(request.getOperation)
    assert(response.getSessionId === "abc123")
  }
}
