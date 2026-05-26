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
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_URL
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryClientProvider, DiscoveryPaths}
import org.apache.kyuubi.server.KyuubiRestFrontendService

private[api] class EngineUIProxyServlet(
    filterEnabled: Boolean,
    registeredEngineUrls: () => Set[(String, Int)]) extends ProxyServlet with Logging {

  @volatile private var cachedRegisteredEngineUrls = Set.empty[(String, Int)]
  @volatile private var cachedAtNanos = 0L

  override def rewriteTarget(request: HttpServletRequest): String = {
    val requestURL = request.getRequestURL
    val requestURI = request.getRequestURI
    // If the request URI is not registered, returns null.
    // Jetty treat null URL as 403 Forbidden error.
    var targetURL: String = null
    extractTargetAddress(requestURI).filter(isRegistered).foreach { case (host, port) =>
      val targetURI = requestURI.stripPrefix(s"/engine-ui/$host:$port") match {
        // for some reason, the proxy can not handle redirect well, as a workaround,
        // we simulate the Spark UI redirection behavior and forcibly rewrite the
        // empty URI to the Spark Jobs page.
        case "" | "/" => "/jobs/"
        case path => path
      }
      val targetQueryString =
        Option(request.getQueryString).filter(StringUtils.isNotEmpty).map(q => s"?$q").getOrElse("")
      targetURL = new URL("http", host, port, targetURI + targetQueryString).toString
    }
    debug(s"rewrite $requestURL => $targetURL")
    targetURL
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    val requestURI = clientRequest.getRequestURI
    extractTargetAddress(requestURI).filter(isRegistered).foreach { case (host, port) =>
      // SPARK-24209: Knox uses X-Forwarded-Context to notify the application the base path
      proxyRequest.header("X-Forwarded-Context", s"/engine-ui/$host:$port")
    }
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  private def isRegistered(target: (String, Int)): Boolean = {
    !filterEnabled || {
      try {
        currentRegisteredEngineUrls().contains(normalizeHostPort(target))
      } catch {
        case NonFatal(e) =>
          warn(s"Failed to validate Engine UI target ${target._1}:${target._2}", e)
          false
      }
    }
  }

  private def currentRegisteredEngineUrls(): Set[(String, Int)] = {
    val now = System.nanoTime()
    val cacheTtlNanos = EngineUIProxyServlet.RegisteredEngineUrlsCacheTtlNanos
    if (now - cachedAtNanos > cacheTtlNanos) {
      synchronized {
        val current = System.nanoTime()
        if (current - cachedAtNanos > cacheTtlNanos) {
          cachedRegisteredEngineUrls = registeredEngineUrls()
          cachedAtNanos = current
        }
      }
    }
    cachedRegisteredEngineUrls
  }

  private val r = "^/engine-ui/([^/:]+):(\\d+)/?.*".r
  private[api] def extractTargetAddress(requestURI: String): Option[(String, Int)] =
    requestURI match {
      case r(host, port) => Some(host -> port.toInt)
      case _ => None
    }

  private def normalizeHostPort(hostPort: (String, Int)): (String, Int) = {
    hostPort._1.toLowerCase(Locale.ROOT) -> hostPort._2
  }
}

private[api] object EngineUIProxyServlet extends Logging {

  // Engine UI pages load many assets through the proxy. Avoid scanning discovery for every request.
  private val RegisteredEngineUrlsCacheTtlNanos = TimeUnit.SECONDS.toNanos(10)

  def apply(fe: KyuubiRestFrontendService): EngineUIProxyServlet = {
    new EngineUIProxyServlet(
      fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_FILTER_ENABLED),
      () => registeredEngineUrls(fe))
  }

  private[api] def registeredEngineUrls(fe: KyuubiRestFrontendService): Set[(String, Int)] = {
    DiscoveryClientProvider.withDiscoveryClient(fe.getConf) { discoveryClient =>
      registeredEngineUrls(discoveryClient, fe.getConf.get(HA_NAMESPACE))
    }
  }

  private[api] def registeredEngineUrls(
      discoveryClient: DiscoveryClient,
      haNamespace: String): Set[(String, Int)] = {
    val registeredUrls = mutable.Set[(String, Int)]()
    val engineNamespacePrefix = s"${haNamespace}_"
    getChildren(discoveryClient, "/").foreach { child =>
      val topLevelNamespace = child.stripPrefix("/").takeWhile(_ != '/')
      if (topLevelNamespace.startsWith(engineNamespacePrefix)) {
        collectRegisteredEngineUrls(
          discoveryClient,
          DiscoveryPaths.makePath(null, topLevelNamespace),
          registeredUrls)
      }
    }
    registeredUrls.toSet
  }

  private def collectRegisteredEngineUrls(
      discoveryClient: DiscoveryClient,
      namespace: String,
      registeredUrls: mutable.Set[(String, Int)]): Unit = {
    getChildren(discoveryClient, namespace).foreach { child =>
      val nodeName = child.split('/').last
      extractEngineUrl(nodeName).flatMap(normalizeEngineUrl).foreach(registeredUrls += _)
      if (!nodeName.contains(";")) {
        collectRegisteredEngineUrls(
          discoveryClient,
          DiscoveryPaths.makePath(namespace, child),
          registeredUrls)
      }
    }
  }

  private def getChildren(discoveryClient: DiscoveryClient, namespace: String): Seq[String] = {
    try {
      Option(discoveryClient.getChildren(namespace)).getOrElse(Nil)
    } catch {
      case NonFatal(_) => Nil
    }
  }

  private def extractEngineUrl(nodeName: String): Option[String] = {
    nodeName
      .split(";")
      .map(_.split("=", 2))
      .collectFirst { case Array(KYUUBI_ENGINE_URL, engineUrl) => engineUrl }
  }

  private[api] def normalizeEngineUrl(engineUrl: String): Option[(String, Int)] = {
    Option(engineUrl).map(_.trim).filter(_.nonEmpty).flatMap { url =>
      Try(new URI(if (url.contains("://")) url else s"http://$url")).toOption.flatMap { uri =>
        for {
          host <- Option(uri.getHost)
          port <- Option(uri.getPort).filter(_ >= 0)
        } yield host.toLowerCase(Locale.ROOT) -> port
      }
    }
  }
}
