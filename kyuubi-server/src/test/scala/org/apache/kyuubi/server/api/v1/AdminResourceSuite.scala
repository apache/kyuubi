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

import java.time.Duration
import java.util.UUID
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{GenericType, MediaType, Response}

import scala.collection.JavaConverters._

import org.mockito.Mockito.lenient
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite, RestFrontendTestHelper, Utils}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_CONNECTION_URL_KEY
import org.apache.kyuubi.engine.{ApplicationManagerInfo, ApplicationState, EngineRef, KubernetesInfo, KyuubiApplicationManager}
import org.apache.kyuubi.engine.EngineType.SPARK_SQL
import org.apache.kyuubi.engine.ShareLevel.{CONNECTION, GROUP, USER}
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.{DiscoveryPaths, ServiceDiscovery}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.plugin.PluginLoader
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.http.util.HttpAuthUtils
import org.apache.kyuubi.server.http.util.HttpAuthUtils.AUTHORIZATION_HEADER
import org.apache.kyuubi.service.authentication.AnonymousAuthenticationProviderImpl
import org.apache.kyuubi.session.SessionType
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2

class AdminResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  private val engineMgr = new KyuubiApplicationManager(None)

  override protected lazy val conf: KyuubiConf = KyuubiConf()
    .set(AUTHENTICATION_METHOD, Seq("CUSTOM"))
    .set(AUTHENTICATION_CUSTOM_CLASS, classOf[AnonymousAuthenticationProviderImpl].getName)
    .set(SERVER_ADMINISTRATORS, Set("admin001"))
    .set(ENGINE_IDLE_TIMEOUT, Duration.ofMinutes(3).toMillis)

  override def beforeAll(): Unit = {
    super.beforeAll()
    engineMgr.initialize(KyuubiConf())
    engineMgr.start()
  }

  override def afterAll(): Unit = {
    engineMgr.stop()
    super.afterAll()
  }

  test("refresh Hadoop configuration of the kyuubi server") {
    var response = webTarget.path("api/v1/admin/refresh/hadoop_conf")
      .request()
      .post(null)
    assert(response.getStatus === 401)

    response = webTarget.path("api/v1/admin/refresh/hadoop_conf")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .post(null)
    assert(response.getStatus === 200)

    response = webTarget.path("api/v1/admin/refresh/hadoop_conf")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader("admin001"))
      .post(null)
    assert(response.getStatus === 200)

    response = webTarget.path("api/v1/admin/refresh/hadoop_conf")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader("admin002"))
      .post(null)
    assert(response.getStatus === 403)
  }

  test("refresh user defaults config of the kyuubi server") {
    var response = webTarget.path("api/v1/admin/refresh/user_defaults_conf")
      .request()
      .post(null)
    assert(response.getStatus === 401)

    response = webTarget.path("api/v1/admin/refresh/user_defaults_conf")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .post(null)
    assert(response.getStatus === 200)
  }

  test("refresh unlimited users of the kyuubi server") {
    var response = webTarget.path("api/v1/admin/refresh/unlimited_users")
      .request()
      .post(null)
    assert(response.getStatus === 401)

    response = webTarget.path("api/v1/admin/refresh/unlimited_users")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .post(null)
    assert(response.getStatus === 200)
  }

  test("refresh deny users of the kyuubi server") {
    var response = webTarget.path("api/v1/admin/refresh/deny_users")
      .request()
      .post(null)
    assert(response.getStatus === 401)

    response = webTarget.path("api/v1/admin/refresh/deny_users")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .post(null)
    assert(response.getStatus === 200)
  }

  test("list/close sessions") {
    val requestObj = new SessionOpenRequest(Map("testConfig" -> "testValue").asJava)

    var response = webTarget.path("api/v1/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    assert(response.getStatus === 200)

    // get session list
    var response2 = webTarget.path("api/v1/admin/sessions").request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    assert(response2.getStatus === 200)
    val sessions1 = response2.readEntity(new GenericType[Seq[SessionData]]() {})
    assert(sessions1.nonEmpty)
    assert(sessions1.head.getConf.get(KYUUBI_SESSION_CONNECTION_URL_KEY) === fe.connectionUrl)

    // close an opened session
    val sessionHandle = response.readEntity(classOf[SessionHandle]).getIdentifier
    response = webTarget.path(s"api/v1/admin/sessions/$sessionHandle").request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .delete()
    assert(response.getStatus === 200)

    // get session list again
    response2 = webTarget.path("api/v1/admin/sessions").request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    assert(200 == response2.getStatus)
    val sessions2 = response2.readEntity(classOf[Seq[SessionData]])
    assert(sessions2.isEmpty)
  }

  test("list sessions/operations with filter") {
    fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "test_user_1",
      "xxxxxx",
      "localhost",
      Map("testConfig" -> "testValue"))

    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "test_user_2",
      "xxxxxx",
      "localhost",
      Map("testConfig" -> "testValue"))

    // list sessions
    var response = webTarget.path("api/v1/admin/sessions")
      .queryParam("users", "admin")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    var sessions = response.readEntity(classOf[Seq[SessionData]])
    assert(response.getStatus === 200)
    assert(sessions.size == 2)

    response = webTarget.path("api/v1/admin/sessions")
      .queryParam("users", "test_user_1,test_user_2")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    sessions = response.readEntity(classOf[Seq[SessionData]])
    assert(response.getStatus === 200)
    assert(sessions.size == 2)

    // list operations
    response = webTarget.path("api/v1/admin/operations")
      .queryParam("users", "test_user_1,test_user_2")
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    var operations = response.readEntity(classOf[Seq[OperationData]])
    assert(operations.size == 2)

    response = webTarget.path("api/v1/admin/operations")
      .queryParam("sessionHandle", sessionHandle.identifier)
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    operations = response.readEntity(classOf[Seq[OperationData]])
    assert(response.getStatus === 200)
    assert(operations.size == 1)

    response = webTarget.path("api/v1/admin/sessions")
      .queryParam("sessionType", SessionType.INTERACTIVE.toString)
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    sessions = response.readEntity(classOf[Seq[SessionData]])
    assert(response.getStatus === 200)
    assert(sessions.size > 0)

    response = webTarget.path("api/v1/admin/sessions")
      .queryParam("sessionType", SessionType.BATCH.toString)
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    sessions = response.readEntity(classOf[Seq[SessionData]])
    assert(response.getStatus === 200)
    assert(sessions.size == 0)

    response = webTarget.path("api/v1/admin/operations")
      .queryParam("sessionType", SessionType.INTERACTIVE.toString)
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    operations = response.readEntity(classOf[Seq[OperationData]])
    assert(response.getStatus === 200)
    assert(operations.size > 0)

    response = webTarget.path("api/v1/admin/operations")
      .queryParam("sessionType", SessionType.BATCH.toString)
      .request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    operations = response.readEntity(classOf[Seq[OperationData]])
    assert(response.getStatus === 200)
    assert(operations.size == 0)
  }

  test("list/close operations") {
    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))
    val operation = fe.be.getCatalogs(sessionHandle)

    // list operations
    var response = webTarget.path("api/v1/admin/operations").request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    assert(response.getStatus === 200)
    var operations = response.readEntity(new GenericType[Seq[OperationData]]() {})
    assert(operations.nonEmpty)
    assert(operations.map(op => op.getIdentifier).contains(operation.identifier.toString))

    // close operation
    response = webTarget.path(s"api/v1/admin/operations/${operation.identifier}").request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .delete()
    assert(response.getStatus === 200)

    // list again
    response = webTarget.path("api/v1/admin/operations").request()
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
      .get()
    operations = response.readEntity(new GenericType[Seq[OperationData]]() {})
    assert(!operations.map(op => op.getIdentifier).contains(operation.identifier.toString))
  }

  test("force to kill engine - user share level") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    val engine =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      Utils.currentUser,
      "default")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)
      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("sharelevel", "USER")
        .queryParam("type", "spark_sql")
        .queryParam("kill", "true")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .delete()

      assert(response.getStatus === 200)
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(client.getChildren(engineSpace).isEmpty, s"refId same with $id?")
      }

      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        val appMgrInfo = ApplicationManagerInfo(None, KubernetesInfo(None, None))
        assert(engineMgr.getApplicationInfo(appMgrInfo, id)
          .exists(_.state == ApplicationState.NOT_FOUND))
      }
    }
  }

  test("delete engine - user share level") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    val engine =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      Utils.currentUser,
      "default")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("sharelevel", "USER")
        .queryParam("type", "spark_sql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .delete()

      assert(response.getStatus === 200)
      assert(client.pathExists(engineSpace))
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(client.getChildren(engineSpace).isEmpty, s"refId same with $id?")
      }

      // kill the engine application
      engineMgr.killApplication(ApplicationManagerInfo(None), id)
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        assert(engineMgr.getApplicationInfo(ApplicationManagerInfo(None), id).exists(
          _.state == ApplicationState.NOT_FOUND))
      }
    }
  }

  test("delete engine - group share level") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, GROUP.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    val engine =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_GROUP_SPARK_SQL",
      fe.asInstanceOf[KyuubiRestFrontendService].sessionManager.groupProvider.primaryGroup(
        Utils.currentUser,
        null),
      "default")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("sharelevel", "GROUP")
        .queryParam("type", "spark_sql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .delete()

      assert(response.getStatus === 200)
      assert(client.pathExists(engineSpace))
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(client.getChildren(engineSpace).isEmpty, s"refId same with $id?")
      }

      // kill the engine application
      engineMgr.killApplication(ApplicationManagerInfo(None), id)
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        assert(engineMgr.getApplicationInfo(ApplicationManagerInfo(None), id).exists(
          _.state == ApplicationState.NOT_FOUND))
      }
    }
  }

  test("delete engine - connection share level") {
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, CONNECTION.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    val id = UUID.randomUUID().toString
    val engine =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)
    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_CONNECTION_SPARK_SQL",
      Utils.currentUser,
      id)

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("sharelevel", "connection")
        .queryParam("type", "spark_sql")
        .queryParam("subdomain", id)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .delete()

      assert(response.getStatus === 200)
    }
  }

  test("delete engine - user share level & proxyUser") {
    val normalUser = "kyuubi"

    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    // In EngineRef, when use hive.server2.proxy.user or kyuubi.session.proxy.user
    // the sessionUser is the proxyUser, and in our test it is normalUser
    val engine =
      new EngineRef(
        conf.clone,
        sessionUser = normalUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)

    // so as the firstChild in engineSpace we use normalUser
    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      normalUser,
      "default")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      def runDeleteEngine(
          kyuubiProxyUser: Option[String],
          hs2ProxyUser: Option[String]): Response = {
        var internalWebTarget = webTarget.path("api/v1/admin/engine")
          .queryParam("sharelevel", "USER")
          .queryParam("type", "SPARK_SQL")

        kyuubiProxyUser.map(username =>
          internalWebTarget = internalWebTarget.queryParam("proxyUser", username))
        hs2ProxyUser.map(username =>
          internalWebTarget = internalWebTarget.queryParam("hive.server2.proxy.user", username))

        internalWebTarget.request(MediaType.APPLICATION_JSON_TYPE)
          .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader("anonymous"))
          .delete()
      }

      // use proxyUser
      val deleteEngineResponse1 = runDeleteEngine(Option(normalUser), None)
      assert(deleteEngineResponse1.getStatus === 403)
      val errorMessage = s"Failed to validate proxy privilege of anonymous for $normalUser"
      assert(deleteEngineResponse1.readEntity(classOf[String]).contains(errorMessage))

      // it should be the same behavior as hive.server2.proxy.user
      val deleteEngineResponse2 = runDeleteEngine(None, Option(normalUser))
      assert(deleteEngineResponse2.getStatus === 403)
      assert(deleteEngineResponse2.readEntity(classOf[String]).contains(errorMessage))

      // when both set, proxyUser takes precedence
      val deleteEngineResponse3 =
        runDeleteEngine(Option(normalUser), Option(s"${normalUser}HiveServer2"))
      assert(deleteEngineResponse3.getStatus === 403)
      assert(deleteEngineResponse3.readEntity(classOf[String]).contains(errorMessage))
    }
  }

  test("list engine - user share level") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    val engine =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      Utils.currentUser,
      "")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("type", "spark_sql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .get

      assert(response.getStatus === 200)
      val engines = response.readEntity(new GenericType[Seq[Engine]]() {})
      assert(engines.size == 1)
      assert(engines(0).getEngineType == "SPARK_SQL")
      assert(engines(0).getSharelevel == "USER")
      assert(engines(0).getSubdomain == "default")

      // kill the engine application
      engineMgr.killApplication(ApplicationManagerInfo(None), id)
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        assert(engineMgr.getApplicationInfo(ApplicationManagerInfo(None), id).exists(
          _.state == ApplicationState.NOT_FOUND))
      }
    }
  }

  test("list engine - group share level") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, GROUP.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    val engine =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_GROUP_SPARK_SQL",
      fe.asInstanceOf[KyuubiRestFrontendService].sessionManager.groupProvider.primaryGroup(
        Utils.currentUser,
        null),
      "")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("type", "spark_sql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .get

      assert(response.getStatus === 200)
      val engines = response.readEntity(new GenericType[Seq[Engine]]() {})
      assert(engines.size == 1)
      assert(engines(0).getEngineType == "SPARK_SQL")
      assert(engines(0).getSharelevel == "GROUP")
      assert(engines(0).getSubdomain == "default")

      // kill the engine application
      engineMgr.killApplication(ApplicationManagerInfo(None), id)
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        assert(engineMgr.getApplicationInfo(ApplicationManagerInfo(None), id).exists(
          _.state == ApplicationState.NOT_FOUND))
      }
    }
  }

  test("list engine - connection share level") {
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, CONNECTION.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_CONNECTION_SPARK_SQL",
      Utils.currentUser,
      "")

    val id1 = UUID.randomUUID().toString
    val engine1 =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id1,
        null)
    val engineSpace1 = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_CONNECTION_SPARK_SQL",
      Utils.currentUser,
      id1)

    val id2 = UUID.randomUUID().toString
    val engine2 =
      new EngineRef(
        conf.clone,
        Utils.currentUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id2,
        null)
    val engineSpace2 = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_CONNECTION_SPARK_SQL",
      Utils.currentUser,
      id2)

    withDiscoveryClient(conf) { client =>
      engine1.getOrCreate(client)
      engine2.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 2)
      assert(client.pathExists(engineSpace1))
      assert(client.pathExists(engineSpace2))

      val response = webTarget.path("api/v1/admin/engine")
        .queryParam("type", "spark_sql")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .get
      assert(response.getStatus === 200)
      val result = response.readEntity(new GenericType[Seq[Engine]]() {})
      assert(result.size == 2)

      val response1 = webTarget.path("api/v1/admin/engine")
        .queryParam("type", "spark_sql")
        .queryParam("subdomain", id1)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .get
      assert(200 == response1.getStatus)
      val result1 = response1.readEntity(new GenericType[Seq[Engine]]() {})
      assert(result1.size == 1)

      // kill the engine application
      engineMgr.killApplication(ApplicationManagerInfo(None), id1)
      engineMgr.killApplication(ApplicationManagerInfo(None), id2)
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        assert(engineMgr.getApplicationInfo(ApplicationManagerInfo(None), id1)
          .exists(_.state == ApplicationState.NOT_FOUND))
        assert(engineMgr.getApplicationInfo(ApplicationManagerInfo(None), id2)
          .exists(_.state == ApplicationState.NOT_FOUND))
      }
    }
  }

  test("list engine - user share level & proxyUser") {
    val normalUser = "kyuubi"

    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")

    // In EngineRef, when use hive.server2.proxy.user or kyuubi.session.proxy.user
    // the sessionUser is the proxyUser, and in our test it is normalUser
    val engine =
      new EngineRef(
        conf.clone,
        sessionUser = normalUser,
        true,
        PluginLoader.loadGroupProvider(conf),
        id,
        null)

    // so as the firstChild in engineSpace we use normalUser
    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      normalUser,
      "")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

      assert(client.pathExists(engineSpace))
      assert(client.getChildren(engineSpace).size == 1)

      def runListEngine(kyuubiProxyUser: Option[String], hs2ProxyUser: Option[String]): Response = {
        var internalWebTarget = webTarget.path("api/v1/admin/engine")
          .queryParam("sharelevel", "USER")
          .queryParam("type", "SPARK_SQL")

        kyuubiProxyUser.map { username =>
          internalWebTarget = internalWebTarget.queryParam("proxyUser", username)
        }
        hs2ProxyUser.map { username =>
          internalWebTarget = internalWebTarget.queryParam("hive.server2.proxy.user", username)
        }

        internalWebTarget.request(MediaType.APPLICATION_JSON_TYPE)
          .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader("anonymous"))
          .get
      }

      // use proxyUser
      val listEngineResponse1 = runListEngine(Option(normalUser), None)
      assert(listEngineResponse1.getStatus === 403)
      val errorMessage = s"Failed to validate proxy privilege of anonymous for $normalUser"
      assert(listEngineResponse1.readEntity(classOf[String]).contains(errorMessage))

      // it should be the same behavior as hive.server2.proxy.user
      val listEngineResponse2 = runListEngine(None, Option(normalUser))
      assert(listEngineResponse2.getStatus === 403)
      assert(listEngineResponse2.readEntity(classOf[String]).contains(errorMessage))

      // when both set, proxyUser takes precedence
      val listEngineResponse3 =
        runListEngine(Option(normalUser), Option(s"${normalUser}HiveServer2"))
      assert(listEngineResponse3.getStatus === 403)
      assert(listEngineResponse3.readEntity(classOf[String]).contains(errorMessage))
    }
  }

  test("list server") {
    // Mock Kyuubi Server
    val serverDiscovery = mock[ServiceDiscovery]
    lenient.when(serverDiscovery.fe).thenReturn(fe)
    val namespace = conf.get(HighAvailabilityConf.HA_NAMESPACE)
    withDiscoveryClient(conf) { client =>
      client.registerService(conf, namespace, serverDiscovery)

      val response = webTarget.path("api/v1/admin/server")
        .request()
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(Utils.currentUser))
        .get

      assert(response.getStatus === 200)
      val result = response.readEntity(new GenericType[Seq[ServerData]]() {})
      assert(result.size == 1)
      val testServer = result.head
      val restFrontendService = fe.asInstanceOf[KyuubiRestFrontendService]

      assert(namespace.equals(testServer.getNamespace.replaceFirst("/", "")))
      assert(restFrontendService.host.equals(testServer.getHost))
      assert(restFrontendService.connectionUrl.equals(testServer.getInstance()))
      assert(!testServer.getAttributes.isEmpty)
      val attributes = testServer.getAttributes
      assert(attributes.containsKey("serverUri") &&
        attributes.get("serverUri").equals(fe.connectionUrl))
      assert(attributes.containsKey("version"))
      assert(attributes.containsKey("sequence"))
      assert("Running".equals(testServer.getStatus))
    }
  }
}
