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

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.RestClientTestHelper
import org.apache.kyuubi.client.{KyuubiRestClient, SessionRestApi}

class SessionRestApiSuite extends RestClientTestHelper {
  test("list session") {
    fe.be.sessionManager.openSession(
      TProtocolVersion.findByValue(1),
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()

    val sessionRestApi = new SessionRestApi(basicKyuubiRestClient)
    val sessions = sessionRestApi.listSessions().asScala
    assert(sessions.size == 1)
    assert(sessions(0).getUser == "admin")
    assert(sessions(0).getIpAddr == "localhost")
    assert(sessions(0).getConf.toString == "{testConfig=testValue}")
  }
}
