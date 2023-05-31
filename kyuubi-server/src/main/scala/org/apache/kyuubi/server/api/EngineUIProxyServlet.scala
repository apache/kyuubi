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

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.api.EngineUIProxyServlet.{CONTEXT_HEADER_KEY, ENGINE_UI_PROXY_PATH}

private[api] class EngineUIProxyServlet extends ProxyServlet with Logging {

  override def rewriteTarget(request: HttpServletRequest): String = {
    var targetUrl = "/no-ui-error"
    val requestUrl = request.getRequestURI
    getTargetAddress(requestUrl).foreach {
      case (host, port) =>
        val subPath = s"/$ENGINE_UI_PROXY_PATH/$host:$port/"
        val targetPath = requestUrl.substring(subPath.length) match {
          case "" => "jobs/"
          case path => path
        }

        targetUrl =
          s"http://${host}:${port}/${targetPath}"
    }
    debug(s"rewrite $requestUrl => $targetUrl")
    targetUrl
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    val addressPair = getTargetAddress(clientRequest.getRequestURI)
    addressPair.foreach { case (host, port) =>
      proxyRequest.header(CONTEXT_HEADER_KEY, s"/engine-ui/$host:$port")
    }
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  private def getTargetAddress(uri: String): Option[(String, Int)] = {
    val url = uri.split("/")
    if (url.length < 3) {
      return None
    }
    val addressPair = url(2).split(":")
    if (addressPair.length != 2) {
      return None
    }
    Some((addressPair(0), addressPair(1).toInt))
  }
}

object EngineUIProxyServlet {
  final private val CONTEXT_HEADER_KEY = "X-Forwarded-Context"
  final val ENGINE_UI_PROXY_PATH = "engine-ui"
}
