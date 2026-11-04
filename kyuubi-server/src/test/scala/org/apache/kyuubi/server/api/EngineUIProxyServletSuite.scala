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

package org.apache.kyuubi.server.api

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{EnumSet, UUID}
import javax.servlet.DispatcherType
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{FilterHolder, ServletContextHandler, ServletHolder}
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{never, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationManagerInfo, ApplicationState, KyuubiApplicationManager}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.ServiceNodeInfo
import org.apache.kyuubi.operation.{BatchJobSubmission, OperationState, OperationStatus}
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.http.authentication.EngineUIAuthenticationFilter
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.{KyuubiBatchSession, KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper
import org.apache.kyuubi.zookeeper.ZookeeperConf.ZK_CLIENT_PORT

class EngineUIProxyServletSuite extends KyuubiFunSuite {
  private val target = new URI("http://engine-host:4173")
  private def servlet(enabled: Boolean = true) =
    new EngineUIProxyServlet(
      enabled,
      (kind, ref) =>
        if (kind == "engines" && ref == "known-ref") Some(EngineUIProxyTarget(target)) else None)

  test("only a known resource resolves to its registered dynamic UI port") {
    val proxy = servlet()
    assert(proxy.rewriteTarget(request("/engine-ui/engines/known-ref/")) ==
      "http://engine-host:4173/jobs/")
    assert(proxy.rewriteTarget(request("/engine-ui/engines/known-ref/sql/", "id=1")) ==
      "http://engine-host:4173/sql/?id=1")
    assert(proxy.rewriteTarget(request("/engine-ui/engines/missing/")) == null)
    assert(proxy.rewriteTarget(request("/engine-ui/engine-host:4173/")) == null)
    assert(proxy.rewriteTarget(request("/engine-ui/engine-host:10250/pods")) == null)
    assert(servlet(false).rewriteTarget(request("/engine-ui/engines/known-ref/")) == null)

  }

  test("proxy retains the registered application path and confines redirects to it") {
    val base = new URI("https://engine-host:8443/proxy/application_1/")
    val proxy =
      new EngineUIProxyServlet(true, (_, _) => Some(EngineUIProxyTarget(base)))
    assert(proxy.rewriteTarget(request("/engine-ui/batches/batch-id/")) == base.toString)
    assert(proxy.rewriteTarget(request("/engine-ui/batches/batch-id/static/app.js")) ==
      "https://engine-host:8443/proxy/application_1/static/app.js")
    val source = base.resolve("jobs/")
    val prefix = "/engine-ui/batches/batch-id"
    assert(EngineUIProxyServlet.rewriteLocation(base, source, prefix, "../stages/?id=2") ==
      Some(s"$prefix/stages/?id=2"))
    assert(EngineUIProxyServlet.rewriteLocation(
      base,
      source,
      prefix,
      "http://engine-host:10250/pods").isEmpty)
    assert(EngineUIProxyServlet.rewriteLocation(base, source, prefix, "/other-app/").isEmpty)
    Seq("../other", "%2e%2e/other", "%252e%252e/other", "a%2f..", "a;param/other")
      .foreach { path =>
        assert(proxy.rewriteTarget(request(s"/engine-ui/batches/batch-id/$path")) == null)
      }
  }

  test("YARN proxy redirects stay within the verified application") {
    val proxy = new URI("http://rm:8088/proxy/application_1_0001/")
    assert(EngineUIProxyServlet.approvedProxyRedirect(
      proxy,
      new URI("http://rm:8088/proxy/redirect/application_1_0001/jobs/?id=2"))
      .contains(new URI("http://rm:8088/proxy/application_1_0001/jobs/?id=2")))
    Seq(
      "http://rm:8088/proxy/application_1_0002/jobs/",
      "http://rm:8088/proxy/application_1_00010/jobs/",
      "http://rm:8088/proxy/application_1_0001/../other/",
      "http://rm:8088/proxy/application_1_0001/%2e%2e/other/",
      "http://rm:8080/proxy/application_1_0001/jobs/",
      "http://foreign:8088/proxy/application_1_0001/jobs/").foreach { value =>
      assert(EngineUIProxyServlet.approvedProxyRedirect(proxy, new URI(value)).isEmpty)
    }
  }

  test("YARN direct access does not query the manager and redirects use only the verified proxy") {
    val backend = new Server(new InetSocketAddress("127.0.0.1", 0))
    val applicationPath = "/proxy/application_1_0001"
    val unapprovedRequests = new java.util.concurrent.atomic.AtomicInteger()
    installServlet(
      backend,
      new HttpServlet {
        override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
          if (request.getRequestURI.startsWith("/proxy/redirect/application_1_0002/")) {
            unapprovedRequests.incrementAndGet()
          } else if (request.getRequestURI.endsWith("/unapproved")) {
            response.sendRedirect("/proxy/redirect/application_1_0002/jobs/")
          } else if (request.getRequestURI.startsWith("/driver/")) {
            response.sendRedirect("/proxy/redirect/application_1_0001/jobs/")
          } else if (request.getRequestURI.endsWith("binary")) {
            response.setContentType("image/png")
            response.getOutputStream.write(Array[Byte](0, 1, -1, 42))
          } else {
            response.setContentType("text/html; charset=utf-8")
            val html = s"<script>setUIRoot('$applicationPath')</script>"
            if (request.getRequestURI.endsWith("gzip")) {
              response.setHeader("Content-Encoding", "gzip")
              val gzip = new java.util.zip.GZIPOutputStream(response.getOutputStream)
              gzip.write(html.getBytes(UTF_8))
              gzip.close()
            } else response.getWriter.print(html)
          }
        }
      })
    val proxy = new Server(new InetSocketAddress("127.0.0.1", 0))
    val client = new HttpClient()
    client.setFollowRedirects(false)
    val queries = new java.util.concurrent.atomic.AtomicInteger()
    try {
      backend.start()
      val address = s"${serverAddress(backend)}"
      installServlet(
        proxy,
        new EngineUIProxyServlet(
          true,
          (_, ref) =>
            Some(EngineUIProxyTarget(
              new URI(address + (if (ref == "direct") "/direct/" else "/driver/")),
              Some(applicationPath),
              () => {
                queries.incrementAndGet()
                Some(new URI(address + applicationPath + "/"))
              }))))
      val conf = new KyuubiConf(false)
        .set(KyuubiConf.AUTHENTICATION_METHOD, Seq("LDAP"))
        .set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, true)
        .set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_AUTH_ENABLED, false)
      proxy.getHandler.asInstanceOf[ServletContextHandler].addFilter(
        new FilterHolder(new EngineUIAuthenticationFilter(conf, proxy = true)),
        "/*",
        EnumSet.of(DispatcherType.REQUEST))
      proxy.start()
      client.start()
      val endpoint = s"${serverAddress(proxy)}/engine-ui/engines/"
      val direct = client.GET(endpoint + "direct/")
      assert(direct.getStatus == 200)
      assert(direct.getContentAsString.contains("setUIRoot('/engine-ui/engines/direct')"))
      assert(client.GET(endpoint + "direct/gzip").getContentAsString == direct.getContentAsString)
      assert(client.GET(endpoint + "direct/binary").getContent.sameElements(
        Array[Byte](0, 1, -1, 42)))
      assert(queries.get() == 0)
      val redirected = client.GET(endpoint + "redirected/")
      assert(redirected.getStatus == 200)
      assert(redirected.getContentAsString.contains("setUIRoot('/engine-ui/engines/redirected')"))
      assert(queries.get() == 1)
      assert(client.GET(endpoint + "redirected/unapproved").getStatus == 502)
      assert(unapprovedRequests.get() == 0)
    } finally {
      client.stop()
      proxy.stop()
      backend.stop()
    }
  }

  test("references identify discovery records without accepting arbitrary discovery paths") {
    val namespace = "/kyuubi_1.13.0-SNAPSHOT_USER_SPARK_SQL/alice/default"
    val id = UUID.randomUUID().toString
    val node = ServiceNodeInfo(namespace, "node", "engine-host", 10009, None, Some(id))
    val ref = EngineUIProxyTargetResolver.reference(node).get
    assert(EngineUIProxyTargetResolver.decodeReference(ref).contains(namespace -> id))
    assert(EngineUIProxyTargetResolver.engineScope(namespace, "kyuubi") ==
      Some("USER" -> "alice"))
    Seq(
      "/other_1.13.0_USER_SPARK_SQL/alice/default",
      namespace + "/../other",
      namespace + "/%2e%2e",
      "/kyuubi_1.13.0_SPARK_SQL/alice/default").foreach { path =>
      assert(EngineUIProxyTargetResolver.engineScope(path, "kyuubi").isEmpty)
    }
    assert(EngineUIProxyTargetResolver.parseTarget("engine-host:4173").contains(target))
    Seq(
      "http://user@engine-host:4173",
      "file:///etc/passwd",
      "host:70000",
      "http://host:80/proxy/../other").foreach { address =>
      assert(EngineUIProxyTargetResolver.parseTarget(address).isEmpty)
    }
  }

  test("resource discovery rejects missing or duplicate instances and checks YARN applications") {
    val conf = new KyuubiConf(false).set(ZK_CLIENT_PORT, 0).set(HA_NAMESPACE, "kyuubi")
    val zk = new EmbeddedZookeeper()
    zk.initialize(conf)
    zk.start()
    conf.set(HA_ADDRESSES, zk.getConnectString)
    val namespace = "/kyuubi_1.13.0-SNAPSHOT_USER_SPARK_SQL/alice/default"
    val id = UUID.randomUUID().toString
    val node = ServiceNodeInfo(namespace, "node", "engine-host", 10009, None, Some(id))
    val ref = EngineUIProxyTargetResolver.reference(node).get
    val manager = mock[KyuubiSessionManager]
    val applicationManager = mock[KyuubiApplicationManager]
    when(manager.applicationManager).thenReturn(applicationManager)
    val frontend = mock[KyuubiRestFrontendService]
    when(frontend.getConf).thenReturn(conf)
    when(frontend.sessionManager).thenReturn(manager)
    val resolver = new EngineUIProxyTargetResolver(frontend)
    try {
      withDiscoveryClient(conf) { discovery =>
        val path = s"$namespace/refId=$id;kyuubi.engine.url=engine-host:4173"
        discovery.create(path, "EPHEMERAL", true)
        discovery.setData(path, "engine-host:10009".getBytes(UTF_8))
        assert(resolver.resolveResource("engines", ref).map(_.uri).contains(target))
        val unknown = EngineUIProxyTargetResolver.reference(
          node.copy(engineRefId = Some(UUID.randomUUID().toString))).get
        assert(resolver.resolveResource("engines", unknown).isEmpty)
        val duplicate = path + ";sequence=other"
        discovery.create(duplicate, "EPHEMERAL", true)
        discovery.setData(duplicate, "engine-host:10009".getBytes(UTF_8))
        assert(resolver.resolveResource("engines", ref).isEmpty)
        discovery.delete(namespace, deleteChildren = true)
        assert(resolver.resolveResource("engines", ref).isEmpty)
        val managerInfo = ApplicationManagerInfo(Some("yarn"))
        val yarnPath = path + ";kyuubi.engine.id=application_1;kyuubi.engine.appMgrInfo=" +
          ApplicationManagerInfo.serialize(managerInfo)
        discovery.create(yarnPath, "EPHEMERAL", true)
        discovery.setData(yarnPath, "engine-host:10009".getBytes(UTF_8))
        val resource = resolver.resolveResource("engines", ref).get
        verify(applicationManager, never()).getApplicationInfo(
          any[ApplicationManagerInfo](),
          any[String](),
          any[Option[String]](),
          any[Option[Long]]())
        val application = ApplicationInfo(
          "application_1",
          "engine",
          ApplicationState.RUNNING,
          Some("http://rm:8088/proxy/application_1/"))
        when(applicationManager.getApplicationInfo(
          eqTo(managerInfo),
          eqTo(id),
          any[Option[String]](),
          any[Option[Long]]()))
          .thenReturn(
            Some(application),
            Some(application.copy(id = "application_2")),
            Some(application.copy(state = ApplicationState.FINISHED)))
        assert(resource.resolveProxy().contains(new URI(application.url.get)))
        assert(resource.resolveProxy().isEmpty)
        assert(resource.resolveProxy().isEmpty)
      }
    } finally {
      resolver.close()
      zk.stop()
    }
    intercept[IllegalArgumentException](resolver.resolveResource("engines", ref))
  }

  test("batch targets require an active local session or metadata record") {
    val id = UUID.randomUUID().toString
    val fe = mock[KyuubiRestFrontendService]
    val manager = mock[KyuubiSessionManager]
    when(fe.sessionManager).thenReturn(manager)
    when(fe.getConf).thenReturn(new KyuubiConf(false)
      .set(KyuubiConf.AUTHENTICATION_METHOD, Seq("NONE")))
    when(manager.getBatchSession(SessionHandle.fromUUID(id))).thenReturn(None)
    val metadata = Metadata(
      id,
      username = "alice",
      state = "RUNNING",
      engineUrl = target.toString,
      engineState = "RUNNING")
    when(manager.getBatchMetadata(id)).thenReturn(Some(metadata))
    val resolver = new EngineUIProxyTargetResolver(fe)
    assert(resolver.resolveResource("batches", id).map(_.uri).contains(target))
    when(manager.getBatchMetadata(id)).thenReturn(Some(metadata.copy(engineState = "FINISHED")))
    assert(resolver.resolveResource("batches", id).map(_.uri).isEmpty)
    when(manager.getBatchMetadata(id)).thenReturn(None)
    assert(resolver.resolveResource("batches", id).map(_.uri).isEmpty)
    val session = mock[KyuubiBatchSession]
    val operation = mock[BatchJobSubmission]
    when(manager.getBatchSession(SessionHandle.fromUUID(id))).thenReturn(Some(session))
    when(session.batchJobSubmissionOp).thenReturn(operation)
    when(operation.getStatus).thenReturn(
      OperationStatus(OperationState.RUNNING, 0L, 0L, 0L, 0L, false))
    when(operation.getApplicationInfo).thenReturn(Some(ApplicationInfo(
      "application_1",
      "batch",
      ApplicationState.RUNNING,
      Some(target.toString))))
    assert(resolver.resolveResource("batches", id).map(_.uri).contains(target))
    when(operation.getStatus).thenReturn(
      OperationStatus(OperationState.FINISHED, 0L, 0L, 0L, 0L, false))
    assert(resolver.resolveResource("batches", id).isEmpty)
    resolver.close()
  }

  test("disabling proxy authentication preserves target validation and credential stripping") {
    val backend = new Server(new InetSocketAddress("127.0.0.1", 0))
    val requests = new java.util.concurrent.atomic.AtomicInteger()
    installServlet(
      backend,
      new HttpServlet {
        override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
          requests.incrementAndGet()
          response.getWriter.print(
            s"${request.getRequestURI}?${request.getQueryString}|" +
              s"${request.getHeader("Authorization")}|${request.getHeader("Cookie")}|" +
              s"${request.getHeader("Proxy-Authorization")}|" +
              s"${request.getHeader("X-Forwarded-Context")}")
        }
      })
    val proxy = new Server(new InetSocketAddress("127.0.0.1", 0))
    val client = new HttpClient()
    try {
      backend.start()
      val id = UUID.randomUUID().toString
      val conf = new KyuubiConf(false)
        .set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, true)
        .set(KyuubiConf.AUTHENTICATION_METHOD, Seq("LDAP"))
      val frontend = mock[KyuubiRestFrontendService]
      val manager = mock[KyuubiSessionManager]
      when(frontend.getConf).thenReturn(conf)
      when(frontend.sessionManager).thenReturn(manager)
      when(manager.getBatchSession(SessionHandle.fromUUID(id))).thenReturn(None)
      when(manager.getBatchMetadata(id)).thenReturn(Some(Metadata(
        id,
        username = "alice",
        state = "RUNNING",
        engineUrl = serverAddress(backend),
        engineState = "RUNNING")))
      installServlet(proxy, EngineUIProxyServlet(frontend))
      proxy.getHandler.asInstanceOf[ServletContextHandler].addFilter(
        new FilterHolder(new EngineUIAuthenticationFilter(conf, proxy = true)),
        "/*",
        EnumSet.of(DispatcherType.REQUEST))
      proxy.start()
      client.start()
      client.getProtocolHandlers.clear()
      val address = serverAddress(proxy) + "/engine-ui/"
      val url = address + s"batches/$id/SQL/execution/?id=0"
      assert(client.GET(url).getStatus == 401)
      assert(requests.get() == 0)
      conf.set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_AUTH_ENABLED, false)
      val anonymous = client.newRequest(url).header("Sec-Fetch-Mode", "navigate").send()
      assert(anonymous.getStatus == 200)
      assert(anonymous.getHeaders.get("Location") == null)
      val response = client.newRequest(address + s"batches/$id/SQL/execution/?id=0")
        .header("Authorization", "Basic unused")
        .header("Proxy-Authorization", "Basic unused")
        .header("Cookie", "unrelated=value")
        .header("X-Forwarded-Context", "/forged")
        .send()
      assert(response.getStatus == 200)
      assert(response.getContentAsString ==
        s"/SQL/execution/?id=0|null|null|null|/engine-ui/batches/$id")
      assert(client.GET(address + "127.0.0.1:10250/pods").getStatus == 403)
      assert(client.GET(address + "engines/other-ref/").getStatus == 403)
      val missing = UUID.randomUUID().toString
      when(manager.getBatchSession(SessionHandle.fromUUID(missing))).thenReturn(None)
      when(manager.getBatchMetadata(missing)).thenReturn(None)
      assert(client.GET(address + s"batches/$missing/").getStatus == 403)
      assert(requests.get() == 2)
    } finally {
      client.stop()
      proxy.stop()
      backend.stop()
    }
  }

  private def serverAddress(server: Server): String =
    s"http://127.0.0.1:${server.getConnectors.head.asInstanceOf[ServerConnector].getLocalPort}"

  private def installServlet(server: Server, servlet: HttpServlet): Unit = {
    val context = new ServletContextHandler()
    context.setContextPath("/")
    val holder = new ServletHolder(servlet)
    holder.setInitOrder(0)
    context.addServlet(holder, "/*")
    server.setHandler(context)
  }

  private def request(uri: String, query: String = null): HttpServletRequest = {
    val request = mock[HttpServletRequest]
    when(request.getRequestURI).thenReturn(uri)
    when(request.getQueryString).thenReturn(query)
    request
  }
}
