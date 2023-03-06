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

import java.util.UUID

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.kyuubi.{KYUUBI_VERSION, RestClientTestHelper}
import org.apache.kyuubi.client.{AdminRestApi, KyuubiRestClient}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryPaths

class AdminRestApiSuite extends RestClientTestHelper {
  test("refresh kyuubi server hadoop conf") {
    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val adminRestApi = new AdminRestApi(spnegoKyuubiRestClient)
    val result = adminRestApi.refreshHadoopConf()
    assert(result === s"Refresh the hadoop conf for ${fe.connectionUrl} successfully.")
  }

  test("engine list/delete operation") {
    val id = UUID.randomUUID().toString
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 180000L)
    conf.set(KyuubiConf.AUTHENTICATION_METHOD, Seq("LDAP", "CUSTOM"))
    val user = ldapUser
    val engine = new EngineRef(conf.clone, user, "grp", id, null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      user,
      "default")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

    }

    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()

    val adminRestApi = new AdminRestApi(basicKyuubiRestClient)
    var engines = adminRestApi.listEngines("spark_sql", "user", "default", "").asScala
    assert(engines.size == 1)
    assert(engines(0).getUser == user)
    assert(engines(0).getVersion == KYUUBI_VERSION)
    assert(engines(0).getEngineType == "SPARK_SQL")
    assert(engines(0).getSharelevel == "USER")
    assert(engines(0).getSubdomain == "default")
    assert(engines(0).getNamespace == engineSpace)
    assert(engines(0).getAttributes.get(KyuubiReservedKeys.KYUUBI_ENGINE_ID).startsWith("local-"))

    val result = adminRestApi.deleteEngine("spark_sql", "user", "default", "")
    assert(result == s"Engine ${engineSpace} is deleted successfully.")

    engines = adminRestApi.listEngines("spark_sql", "user", "default", "").asScala
    assert(engines.size == 0)
  }
}
