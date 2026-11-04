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

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import javax.servlet.{FilterChain, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse, HttpServletResponseWrapper}

import scala.util.Try

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_REST_ENGINE_UI_PROXY_AUTH_ENABLED, FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, FRONTEND_REST_UI_ENABLED}

/** Carries Web UI Basic credentials to the existing authentication chain on every proxy request. */
private[server] class EngineUIAuthenticationFilter(conf: KyuubiConf, proxy: Boolean)
  extends AuthenticationFilter(conf) {
  import EngineUIAuthenticationFilter._

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]
    if (!proxy) {
      if (httpRequest.getRequestURI == "/api/v1/logout" && httpRequest.getMethod == "POST") {
        setCookie(httpRequest, httpResponse, "", clear = true)
      }
      super.doFilter(request, response, chain)
      return
    }
    // The servlet still enforces feature availability and target validation without authentication.
    if (!conf.get(FRONTEND_REST_ENGINE_UI_PROXY_ENABLED) ||
      !conf.get(FRONTEND_REST_ENGINE_UI_PROXY_AUTH_ENABLED)) {
      chain.doFilter(request, response)
      return
    }
    // Anonymous deployments and explicit Authorization keep their existing authentication path.
    if (getMatchedHandler(null).nonEmpty || httpRequest.getHeader("Authorization") != null) {
      super.doFilter(request, response, chain)
      return
    }
    // Pure Kerberos deployments initiate browser SPNEGO instead of the Basic login form.
    if (getMatchedHandler("Basic").isEmpty && getMatchedHandler("Negotiate").nonEmpty) {
      super.doFilter(request, response, chain)
      return
    }
    val cookies = Option(httpRequest.getCookies).toSeq.flatMap(_.toSeq)
      .filter(_.getName == COOKIE)
    val credentials = if (cookies.size == 1) {
      basicCredentials(cookies.head.getValue).filter(value =>
        getMatchedHandler("Basic " + value).exists(_.authScheme == AuthSchemes.BASIC))
    } else None
    val navigation = conf.get(FRONTEND_REST_UI_ENABLED) && httpRequest.getMethod == "GET" &&
      (httpRequest.getHeader("Sec-Fetch-Mode") == "navigate" ||
        httpRequest.getHeader("Sec-Fetch-Mode") == null &&
        Option(httpRequest.getHeader("Accept")).exists(_.contains("text/html"))) &&
      httpRequest.getHeader("X-Requested-With") == null
    def login(): Unit = {
      val target = httpRequest.getRequestURI +
        Option(httpRequest.getQueryString).map("?" + _).getOrElse("")
      httpResponse.setHeader("Cache-Control", "no-store")
      httpResponse.sendRedirect("/ui/?returnTo=" + URLEncoder.encode(target, UTF_8.name()))
    }
    credentials match {
      case None if navigation => login()
      case None => super.doFilter(request, response, chain)
      case Some(value) =>
        val safeMethod = Set("GET", "HEAD", "OPTIONS").contains(httpRequest.getMethod)
        if (!safeMethod && !sameOrigin(httpRequest)) {
          httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN)
          return
        }
        val authenticatedRequest = new HttpServletRequestWrapper(httpRequest) {
          override def getHeader(name: String): String = {
            if (name.equalsIgnoreCase("Authorization")) "Basic " + value
            else super.getHeader(name)
          }
        }
        val authenticationResponse = new HttpServletResponseWrapper(httpResponse) {
          override def sendError(status: Int, message: String): Unit = {
            setCookie(httpRequest, httpResponse, "", clear = true)
            if (navigation && Set(
                HttpServletResponse.SC_UNAUTHORIZED,
                HttpServletResponse.SC_FORBIDDEN).contains(status)) {
              login()
            } else {
              super.sendError(status, message)
            }
          }
        }
        // Only authentication failures trigger login. Preserve errors from the target resource.
        val authenticatedChain = new FilterChain {
          override def doFilter(req: ServletRequest, res: ServletResponse): Unit =
            chain.doFilter(req, httpResponse)
        }
        super.doFilter(authenticatedRequest, authenticationResponse, authenticatedChain)
    }
  }

  override protected def doFilter(
      chain: FilterChain,
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    if (!proxy && conf.get(FRONTEND_REST_ENGINE_UI_PROXY_ENABLED) &&
      conf.get(FRONTEND_REST_ENGINE_UI_PROXY_AUTH_ENABLED) &&
      request.getRequestURI == "/api/v1/ping" && getMatchedHandler(null).isEmpty &&
      AuthenticationFilter.getAuthType == AuthSchemes.BASIC.toString) {
      val authorization = request.getHeader("Authorization")
      basicCredentials(authorization.substring("Basic".length).trim) match {
        case Some(value) => setCookie(request, response, value)
        case None =>
          response.sendError(
            HttpServletResponse.SC_BAD_REQUEST,
            "Basic credentials cannot be represented by the Engine UI browser cookie")
          return
      }
    }
    super.doFilter(chain, request, response)
  }
}

private[server] object EngineUIAuthenticationFilter {
  private val COOKIE = "kyuubi.engine.ui.credentials"
  // Leave room for the name and attributes within the browser cookie size limit.
  private val MAX_CREDENTIAL_LENGTH = 3800

  private def sameOrigin(request: HttpServletRequest): Boolean = {
    // Browsers compute this before a TLS-terminating proxy rewrites the transport or Host.
    // Do not trust arbitrary Forwarded headers to turn a cross-origin request into same-origin.
    Option(request.getHeader("Sec-Fetch-Site")) match {
      case Some(site) => site == "same-origin"
      case None =>
        Option(request.getHeader("Origin")).flatMap(value => Try(new URI(value)).toOption)
          .exists { origin =>
            val port = if (origin.getPort != -1) origin.getPort
            else if (origin.getScheme == "https") 443 else 80
            origin.getScheme == request.getScheme && origin.getHost != null &&
            origin.getHost.equalsIgnoreCase(request.getServerName) &&
            port == request.getServerPort && origin.getUserInfo == null &&
            Option(origin.getRawPath).forall(_.isEmpty) &&
            origin.getRawQuery == null && origin.getRawFragment == null
          }
    }
  }

  private def basicCredentials(value: String): Option[String] = {
    if (value.length > MAX_CREDENTIAL_LENGTH || !value.matches("[A-Za-z0-9+/]+={0,2}")) return None
    Try(new String(Base64.getDecoder.decode(value), UTF_8)).toOption
      .filter { decoded =>
        val separator = decoded.indexOf(':')
        separator > 0 && separator < decoded.length - 1
      }.map(_ => value)
  }

  private def setCookie(
      request: HttpServletRequest,
      response: HttpServletResponse,
      value: String,
      clear: Boolean = false): Unit = {
    val secure = if (request.isSecure) "; Secure" else ""
    val expires = if (clear) "; Max-Age=0" else ""
    response.addHeader(
      "Set-Cookie",
      s"$COOKIE=$value; Path=/engine-ui; HttpOnly; SameSite=Strict$secure$expires")
    response.setHeader("Cache-Control", "no-store")
  }
}
