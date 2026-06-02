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

import java.net.{URI, URL}
import java.util.Locale
import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

import scala.util.Try

import org.apache.commons.lang3.StringUtils
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.KyuubiRestFrontendService

private[api] class EngineUIProxyServlet(
    proxyEnabled: Boolean,
    allowedHosts: Set[String]) extends ProxyServlet with Logging {

  private val allowedHostPatterns =
    allowedHosts.map(EngineUIProxyServlet.normalizeHost).filter(_.nonEmpty).map(
      EngineUIProxyServlet.wildcardHostPattern)

  override def rewriteTarget(request: HttpServletRequest): String = {
    val requestURL = request.getRequestURL
    val requestURI = request.getRequestURI
    // If proxying is enabled and the request URI host is not allowed, return null so Jetty
    // responds with 403 Forbidden.
    val targetURL = allowedTarget(requestURI).map { targetAddress =>
      val targetURI = targetAddress.path match {
        // for some reason, the proxy can not handle redirect well, as a workaround,
        // we simulate the Spark UI redirection behavior and forcibly rewrite the
        // empty URI to the Spark Jobs page.
        case "" | "/" => "/jobs/"
        case path => path
      }
      val targetQueryString =
        Option(
          request.getQueryString).filter(StringUtils.isNotEmpty).map(q => s"?$q").getOrElse("")
      new URL(
        "http",
        targetAddress.host,
        targetAddress.port,
        targetURI + targetQueryString).toString
    }.orNull
    debug(s"rewrite $requestURL => $targetURL")
    targetURL
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    allowedTarget(clientRequest.getRequestURI).foreach {
      case TargetAddress(host, port, _) =>
        // SPARK-24209: Knox uses X-Forwarded-Context to notify the application the base path
        proxyRequest.header("X-Forwarded-Context", s"/engine-ui/$host:$port")
    }
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  private def isAllowedHost(host: String): Boolean =
    proxyEnabled && {
      val normalizedHost = EngineUIProxyServlet.normalizeHost(host)
      allowedHostPatterns.exists(_.matcher(normalizedHost).matches())
    }

  private def allowedTarget(requestURI: String): Option[TargetAddress] =
    extractTargetAddress(requestURI).filter(target => isAllowedHost(target.host))

  private[api] def extractTargetAddress(requestURI: String): Option[TargetAddress] = {
    val target = requestURI.stripPrefix("/engine-ui/")
    if (target == requestURI) {
      None
    } else {
      Try(new URI(s"http://$target")).toOption.flatMap { uri =>
        for {
          host <- Option(uri.getHost)
          port <- Option(uri.getPort).filter(_ >= 0)
        } yield TargetAddress(
          host,
          port,
          Option(uri.getRawPath).filter(_.nonEmpty).getOrElse("/"))
      }
    }
  }

}

private[api] case class TargetAddress(host: String, port: Int, path: String)

private[api] object EngineUIProxyServlet {

  def apply(fe: KyuubiRestFrontendService): EngineUIProxyServlet = {
    new EngineUIProxyServlet(
      fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED),
      fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_HOSTS).toSet)
  }

  private[api] def normalizeHost(host: String): String = host.trim.toLowerCase(Locale.ROOT)

  private[api] def wildcardHostPattern(hostPattern: String): Pattern = {
    Pattern.compile(hostPattern.map {
      case '*' => ".*"
      case c => Pattern.quote(c.toString)
    }.mkString("^", "", "$"))
  }
}
