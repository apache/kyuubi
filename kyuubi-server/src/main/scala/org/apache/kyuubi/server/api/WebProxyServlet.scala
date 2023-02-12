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

import java.net.URI
import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.client.api.Response
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

private[api] class WebProxyServlet(conf: KyuubiConf) extends ProxyServlet with Logging {
  val ATTR_TARGET_HOST = classOf[ProxyServlet].getSimpleName + ".targetHost"
  val ATTR_TARGET_URI = classOf[ProxyServlet].getSimpleName() + ".targetUri"

  override def rewriteTarget(request: HttpServletRequest): String = {
    var targetUrl = "/no-ui-error"
    val requestUrl = request.getRequestURI
    logger.info("requestUrl is {}", requestUrl)
    val url = requestUrl.split("/")
    logger.info("url is {}", url)
    if (url != null && url.length > 0) {
      val ipAddress = url(2).split(":")(0)
      val port = url(2).split(":")(1).toInt
      val path = requestUrl.substring(("/" + url(1) + "/" + url(2) + "/").length)
      targetUrl = String.format(
        "http://%s:%s/%s/",
        ipAddress,
        port.toString,
        path)
      logger.info("ui -> {}", targetUrl)
    }
    targetUrl
  }

  override def filterServerResponseHeader(
      clientRequest: HttpServletRequest,
      serverResponse: Response,
      headerName: String,
      headerValue: String): String = {

    if (headerName.equalsIgnoreCase("location")) {
      val newHeader = createProxyLocationHeader(
        clientRequest.getContextPath,
        headerValue,
        clientRequest,
        serverResponse.getRequest().getURI())
      logger.info("new location header is", newHeader)
      if (newHeader != null) {
        return newHeader
      }
    }
    super.filterServerResponseHeader(
      clientRequest,
      serverResponse,
      headerName,
      headerValue)
  }

  def createProxyLocationHeader(
      prefix: String,
      headerValue: String,
      clientRequest: HttpServletRequest,
      targetUri: URI): String = {
    val toReplace = targetUri.getScheme() + "://" + targetUri.getAuthority()
    if (headerValue.startsWith(toReplace)) {
      clientRequest.getScheme() + "://" + clientRequest.getHeader("host") +
        prefix + headerValue.substring(toReplace.length())
    } else {
      null
    }

  }
}
