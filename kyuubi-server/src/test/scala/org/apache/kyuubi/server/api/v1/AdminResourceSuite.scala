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

import java.util.{Base64, UUID}
import javax.ws.rs.core.MediaType

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite, RestFrontendTestHelper, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.engine.EngineType.SPARK_SQL
import org.apache.kyuubi.engine.ShareLevel.USER
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryPaths
import org.apache.kyuubi.server.http.authentication.AuthenticationHandler.AUTHORIZATION_HEADER

class AdminResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {
  test("refresh Hadoop configuration of the kyuubi server") {
    var response = webTarget.path("api/v1/admin/refresh/hadoop_conf")
      .request()
      .post(null)
    assert(405 == response.getStatus)

    val adminUser = Utils.currentUser
    val encodeAuthorization = new String(
      Base64.getEncoder.encode(
        s"$adminUser:".getBytes()),
      "UTF-8")
    response = webTarget.path("api/v1/admin/refresh/hadoop_conf")
      .request()
      .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
      .post(null)
    assert(200 == response.getStatus)
  }

  test("delete engine") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 180000L)
    val engine = new EngineRef(conf, Utils.currentUser, id, null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      Utils.currentUser,
      Array("default"))

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      var child = client.getChildren(engineSpace)
      assert(child.size == 1)

      val adminUser = Utils.currentUser
      val encodeAuthorization = new String(
        Base64.getEncoder.encode(
          s"$adminUser:".getBytes()),
        "UTF-8")
      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("user", adminUser)
        .queryParam("namespace", "kyuubi_test")
        .queryParam("sharelevel", "USER")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, s"BASIC $encodeAuthorization")
        .delete()

      assert(200 == response.getStatus)
      assert(client.pathExists(engineSpace))
      child = client.getChildren(engineSpace)
      assert(child.size == 0)

      val result = response.readEntity(classOf[String])
      assert(result.contains(s"Engine ${engineSpace} is deleted successfully."))
    }
  }

}
