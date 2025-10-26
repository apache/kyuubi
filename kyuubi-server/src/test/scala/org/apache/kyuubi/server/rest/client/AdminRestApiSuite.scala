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

import org.mockito.Mockito.lenient
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.{KYUUBI_VERSION, RestClientTestHelper}
import org.apache.kyuubi.client.{AdminRestApi, KyuubiRestClient}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.{DiscoveryPaths, ServiceDiscovery}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.plugin.PluginLoader
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2

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
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")
    val user = ldapUser
    val engine =
      new EngineRef(conf.clone, user, true, PluginLoader.loadGroupProvider(conf), id, null)

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

    // kill engine to release memory quickly
    val result = adminRestApi.deleteEngine("spark_sql", "user", "default", "", true)
    assert(result startsWith s"Engine ${engineSpace} refId=${id} is deleted successfully.")

    engines = adminRestApi.listEngines("spark_sql", "user", "default", "").asScala
    assert(engines.isEmpty)
  }

  test("list/close session") {
    fe.be.sessionManager.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val adminRestApi = new AdminRestApi(spnegoKyuubiRestClient)

    // list sessions
    var sessions = adminRestApi.listSessions().asScala
    assert(sessions.nonEmpty)
    assert(sessions.head.getUser == "admin")

    // close session
    val response = adminRestApi.closeSession(sessions.head.getIdentifier)
    assert(response.contains("success"))

    // list again
    sessions = adminRestApi.listSessions().asScala
    assert(sessions.isEmpty)
  }

  test("list/close operation") {
    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))
    val operation = fe.be.getCatalogs(sessionHandle)

    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val adminRestApi = new AdminRestApi(spnegoKyuubiRestClient)

    // list operations
    var operations = adminRestApi.listOperations().asScala
    assert(operations.nonEmpty)
    assert(operations.map(op => op.getIdentifier).contains(operation.identifier.toString))

    // close operation
    val response = adminRestApi.closeOperation(operation.identifier.toString)
    assert(response.contains("success"))

    // list again
    operations = adminRestApi.listOperations().asScala
    assert(!operations.map(op => op.getIdentifier).contains(operation.identifier.toString))

  }

  test("list server") {
    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val adminRestApi = new AdminRestApi(spnegoKyuubiRestClient)

    // Mock Kyuubi Server
    val serverDiscovery = mock[ServiceDiscovery]
    lenient.when(serverDiscovery.fe).thenReturn(fe)
    val namespace = conf.get(HighAvailabilityConf.HA_NAMESPACE)
    withDiscoveryClient(conf) { client =>
      client.registerService(conf, namespace, serverDiscovery)

      val servers = adminRestApi.listServers().asScala
      assert(servers.nonEmpty)
      assert(servers.map(s => s.getInstance()).contains(server.frontendServices.last.connectionUrl))
    }
  }

  test("get server event") {
    val spnegoKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
        .spnegoHost("localhost")
        .build()
    val adminRestApi = new AdminRestApi(spnegoKyuubiRestClient)
    assert(adminRestApi.getServerEvent.getStartTime > 0)
  }
}
