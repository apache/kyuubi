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

import java.net.URL
import javax.servlet.http.HttpServletRequest

import org.apache.commons.lang3.StringUtils
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging

private[api] class EngineUIProxyServlet extends ProxyServlet with Logging {

  override def rewriteTarget(request: HttpServletRequest): String = {
    val requestURL = request.getRequestURL
    val requestURI = request.getRequestURI
    var targetURL = "/no-ui-error"
    extractTargetAddress(requestURI).foreach { case (host, port) =>
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
    extractTargetAddress(requestURI).foreach { case (host, port) =>
      // SPARK-24209: Knox uses X-Forwarded-Context to notify the application the base path
      proxyRequest.header("X-Forwarded-Context", s"/engine-ui/$host:$port")
    }
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  private val r = "^/engine-ui/([^/:]+):(\\d+)/?.*".r
  private def extractTargetAddress(requestURI: String): Option[(String, Int)] =
    requestURI match {
      case r(host, port) => Some(host -> port.toInt)
      case _ => None
    }
}
