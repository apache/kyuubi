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

import java.net.URLDecoder
import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

private[api] class EngineUIProxyServlet(conf: KyuubiConf) extends ProxyServlet with Logging {
  var ipAddress: String = _
  var port: Int = _
  val CONTEXT_HEADER_KEY = "X-Forwarded-Context"
  var CONTEXT_HEADER_VALUE = ""

  override def rewriteTarget(request: HttpServletRequest): String = {
    var targetUrl = "/no-ui-error"
    val requestUrl = request.getRequestURI
    logger.info("requestUrl is {}", requestUrl)
    val url = requestUrl.split("/")
    logger.info("url is {}", url)
    if (url != null && url.length > 0) {
      ipAddress = url(2).split(":")(0)
      port = url(2).split(":")(1).toInt
      var targetPath = "jobs/"
      val subPath = String.format(
        "/%s/%s/",
        url(1),
        url(2))
      if (requestUrl.substring(subPath.length).nonEmpty) {
        targetPath = requestUrl.substring(subPath.length)
      }
      targetUrl = String.format(
        "http://%s:%s/%s%s",
        ipAddress,
        port.toString,
        targetPath,
        getQueryString(request))
      CONTEXT_HEADER_VALUE = s"/engine-ui/$ipAddress:$port"
      logger.info("ui -> {}", targetUrl)
    }
    targetUrl
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    proxyRequest.header(CONTEXT_HEADER_KEY, CONTEXT_HEADER_VALUE)
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  def getQueryString(servletRequest: HttpServletRequest): String = {
    val result = new StringBuilder()
    val queryString = servletRequest.getQueryString()
    if (queryString != null && queryString.length() > 0) {
      result.append('?')
      result.append(URLDecoder.decode(queryString, "UTF-8"))
    }
    result.toString()
  }

}
