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

package org.apache.kyuubi.server.http.authentication

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Base64, EnumSet}
import javax.servlet.{DispatcherType, FilterChain}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{FilterHolder, ServletContextHandler, ServletHolder}
import org.mockito.Mockito.{verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{AnonymousAuthenticationProviderImpl, AuthTypes, WithLdapServer}

class AuthenticationFilterSuite extends WithLdapServer {
  override protected val ldapUser: String = "user"
  override protected val ldapUserPasswd: String = "password"

  test("add auth handler and destroy") {
    val filter = new AuthenticationFilter(KyuubiConf())
    filter.addAuthHandler(new BasicAuthenticationHandler(null))
    assert(filter.authSchemeHandlers.isEmpty)
    filter.addAuthHandler(new BasicAuthenticationHandler(AuthTypes.LDAP))
    assert(filter.authSchemeHandlers.size == 1)
    filter.addAuthHandler(new BasicAuthenticationHandler(AuthTypes.LDAP))
    assert(filter.authSchemeHandlers.size == 1)
    filter.addAuthHandler(new KerberosAuthenticationHandler())
    assert(filter.authSchemeHandlers.size == 1)
    filter.addAuthHandler(new BearerAuthenticationHandler(classOf[KyuubiConf].getName))
    assert(filter.authSchemeHandlers.size == 1)
    filter.addAuthHandler(new BearerAuthenticationHandler(
      classOf[AnonymousAuthenticationProviderImpl].getName))
    assert(filter.authSchemeHandlers.size == 2)
    filter.destroy()
    assert(filter.authSchemeHandlers.isEmpty)
  }
  test("Kerberos browser navigation receives a SPNEGO challenge") {
    val conf = new KyuubiConf(false).set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS"))
      .set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, true)
    Seq(false, true).foreach { proxy =>
      val filter = new EngineUIAuthenticationFilter(conf, proxy)
      filter.addAuthHandler(new KerberosAuthenticationHandler {
        override def init(conf: KyuubiConf): Unit = {}
        override def authenticationSupported: Boolean = true
      })
      val request = mock[HttpServletRequest]
      val response = mock[HttpServletResponse]
      val chain = mock[FilterChain]
      when(request.getMethod).thenReturn("GET")
      when(request.getRequestURI).thenReturn("/engine-ui/engines/ref/SQL/execution/")
      when(request.getHeader("Sec-Fetch-Mode")).thenReturn("navigate")
      try {
        filter.doFilter(request, response, chain)
        verify(response).setHeader("WWW-Authenticate", "Negotiate")
        verify(response).sendError(401, "No auth scheme matched for null")
      } finally {
        filter.destroy()
      }
    }
  }

  test("anonymous proxy modes do not require browser credentials") {
    Seq("NONE", "NOSASL").foreach { method =>
      val filter = new EngineUIAuthenticationFilter(
        new KyuubiConf(false).set(KyuubiConf.AUTHENTICATION_METHOD, Seq(method))
          .set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, true),
        true)
      val request = mock[HttpServletRequest]
      val response = mock[HttpServletResponse]
      val chain = mock[FilterChain]
      when(request.getMethod).thenReturn("GET")
      filter.init(null)
      try {
        filter.doFilter(request, response, chain)
        verify(chain).doFilter(request, response)
      } finally {
        filter.destroy()
        AuthenticationFilter.HTTP_CLIENT_USER_NAME.remove()
      }
    }
  }

  test("browser credentials authenticate resource requests and preserve the login return path") {
    val conf = new KyuubiConf(false)
      .set(KyuubiConf.AUTHENTICATION_METHOD, Seq("LDAP"))
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN, "uid=%s,ou=users")
      .set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, true)
    val server = new Server(new InetSocketAddress("127.0.0.1", 0))
    val client = new HttpClient()
    client.setCookieStore(new org.eclipse.jetty.util.HttpCookieStore.Empty())
    val authenticatedRequests = new java.util.concurrent.atomic.AtomicInteger()
    val credentials = Base64.getEncoder.encodeToString("user:password".getBytes(UTF_8))
    try {
      val context = new ServletContextHandler()
      context.setContextPath("/")
      Seq(false, true).foreach { proxy =>
        context.addFilter(
          new FilterHolder(new EngineUIAuthenticationFilter(conf, proxy)),
          if (proxy) "/engine-ui/*" else "/api/v1/*",
          EnumSet.of(DispatcherType.REQUEST))
      }
      context.addServlet(
        new ServletHolder(new HttpServlet {
          override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
            authenticatedRequests.incrementAndGet()
            if (req.getRequestURI.endsWith("missing")) {
              res.sendError(403, "missing resource")
            } else {
              res.getWriter.print(AuthenticationFilter.getUserName + "|" + req.getQueryString)
            }
          }
          override def doPost(req: HttpServletRequest, res: HttpServletResponse): Unit =
            doGet(req, res)
        }),
        "/*")
      server.setHandler(context)
      server.start()
      client.start()
      client.getProtocolHandlers.clear()
      val login = client.newRequest(serverAddress(server) + "/api/v1/ping")
        .header("Authorization", "Basic " + credentials).send()
      assert(login.getStatus == 200)
      val issued = login.getHeaders.get("Set-Cookie")
      assert(issued.contains("HttpOnly") && issued.contains("SameSite=Strict"))
      assert(issued.contains("Path=/engine-ui") && !issued.contains("Max-Age"))
      val cookie = issued.takeWhile(_ != ';')
      assert(cookie == "kyuubi.engine.ui.credentials=" + credentials)
      val origin = serverAddress(server)
      val url = origin + "/engine-ui/engines/ref/SQL/execution/?id=0"
      assert(client.GET(url).getStatus == 401)
      val navigation = client.newRequest(url).header("Sec-Fetch-Mode", "navigate").send()
      assert(navigation.getStatus == 302)
      assert(navigation.getHeaders.get("Location").endsWith(
        "/ui/?returnTo=%2Fengine-ui%2Fengines%2Fref%2FSQL%2Fexecution%2F%3Fid%3D0"))
      assert(authenticatedRequests.get() == 1) // Only the authenticated ping reached the servlet.
      val response = client.newRequest(url).header("Cookie", cookie).send()
      assert(response.getStatus == 200)
      assert(response.getContentAsString == "user|id=0")
      val invalid = "kyuubi.engine.ui.credentials=" +
        Base64.getEncoder.encodeToString("user:wrong-password".getBytes(UTF_8))
      assert(client.newRequest(url).header("Cookie", invalid).send().getStatus == 403)
      assert(client.newRequest(url).header("Cookie", cookie)
        .header("Authorization", "Basic " + invalid.split("=", 2)(1))
        .send().getStatus == 403)
      assert(authenticatedRequests.get() == 2)
      assert(client.newRequest(url).method("POST").header("Cookie", cookie)
        .send().getStatus == 403)
      assert(client.newRequest(url).method("POST").header("Cookie", cookie)
        .header("Origin", "https://foreign.invalid").send().getStatus == 403)
      assert(client.newRequest(url).method("POST").header("Cookie", cookie)
        .header("Origin", "https://foreign.invalid")
        .header("X-Forwarded-Proto", "https").header("X-Forwarded-Host", "foreign.invalid")
        .send().getStatus == 403)
      assert(client.newRequest(url).method("POST").header("Cookie", cookie)
        .header("Origin", origin).send().getStatus == 200)
      // The browser's origin is HTTPS even though the TLS terminator connects over HTTP.
      assert(client.newRequest(url).method("POST").header("Cookie", cookie)
        .header("Origin", "https://public.example")
        .header("Sec-Fetch-Site", "same-origin").send().getStatus == 200)
      assert(client.newRequest(url).method("POST").header("Cookie", cookie)
        .header("Origin", origin).header("Sec-Fetch-Site", "cross-site")
        .send().getStatus == 403)
      assert(client.newRequest(origin + "/engine-ui/engines/ref/missing")
        .header("Cookie", cookie).header("Sec-Fetch-Mode", "navigate")
        .send().getStatus == 403)
      assert(client.newRequest(origin + "/api/v1/ping").header("Cookie", cookie)
        .send().getStatus == 401)
      conf.set(KyuubiConf.FRONTEND_REST_UI_ENABLED, false)
      try {
        val noWebUI = client.newRequest(url).header("Sec-Fetch-Mode", "navigate").send()
        assert(noWebUI.getStatus == 401)
        assert(noWebUI.getHeaders.get("Location") == null)
        val expired = client.newRequest(url).header("Cookie", invalid)
          .header("Sec-Fetch-Mode", "navigate").send()
        assert(expired.getStatus == 403)
        assert(expired.getHeaders.get("Location") == null)
        assert(expired.getHeaders.get("Set-Cookie").contains("Max-Age=0"))
        assert(client.newRequest(url).header("Cookie", cookie)
          .header("Sec-Fetch-Mode", "navigate").send().getStatus == 200)
      } finally {
        conf.set(KyuubiConf.FRONTEND_REST_UI_ENABLED, true)
      }
      val logout = client.newRequest(origin + "/api/v1/logout").method("POST").send()
      assert(logout.getHeaders.get("Set-Cookie").contains("Max-Age=0"))
      conf.set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_AUTH_ENABLED, false)
      assert(client.GET(origin + "/api/v1/ping").getStatus == 401)
      val noProxyCookie = client.newRequest(origin + "/api/v1/ping")
        .header("Authorization", "Basic " + credentials).send()
      assert(noProxyCookie.getStatus == 200)
      assert(noProxyCookie.getHeaders.get("Set-Cookie") == null)
      conf.set(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, false)
      val disabled = client.newRequest(url).header("Sec-Fetch-Mode", "navigate").send()
      assert(
        disabled.getStatus == 200
      ) // With the proxy disabled, the filter leaves it to the servlet.
      assert(disabled.getHeaders.get("Location") == null)
    } finally {
      client.stop()
      server.stop()
    }
  }

  private def serverAddress(server: Server): String =
    s"http://127.0.0.1:${server.getConnectors.head.asInstanceOf[ServerConnector].getLocalPort}"
}
