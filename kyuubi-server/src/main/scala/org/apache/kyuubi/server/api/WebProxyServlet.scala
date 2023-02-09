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
import org.apache.kyuubi.config.KyuubiConf

private[api] class WebProxyServlet(conf: KyuubiConf) extends ProxyServlet with Logging {
  val ATTR_TARGET_HOST = classOf[ProxyServlet].getSimpleName + ".targetHost"
  val ATTR_TARGET_URI = classOf[ProxyServlet].getSimpleName() + ".targetUri"

  // http://10.252.125.84:30029/proxy/spark-3137fa8624ad23ef-driver-svc:4045/jobs/
  override def rewriteTarget(request: HttpServletRequest): String = {
    var targetUrl = "/no-ui-error"
    val requestUrl = request.getRequestURI
    logger.info("requestUrl is {}", requestUrl)
    val url = requestUrl.split("/")
    logger.info("url is {}", url)
    if (url != null && url.length > 0) {
      val ipAddress = url(1).split(":")(1)
      val port = url(1).split(":")(2).toInt
      val path = url(2)
      /* request.setAttribute(
        ATTR_TARGET_HOST,
        new HttpHost(ipAddress, port, "http"))*/
      targetUrl =
        String.format(
          "http://%s:%s/%s/",
          ipAddress,
          port.toString,
          path)
      /*      request.setAttribute(
        ATTR_TARGET_URI,
        targetUrl)*/
      logger.info("ui -> {}", targetUrl)
    }
    targetUrl
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    val forHeaderName = "X-Forwarded-For"
    var forHeader = clientRequest.getRemoteAddr()
    val existingForHeader = clientRequest.getHeader(forHeaderName)
    if (existingForHeader != null) {
      forHeader = existingForHeader + ", " + forHeader
    }
    proxyRequest.header(forHeaderName, forHeader)
    val protoHeaderName = "X-Forwarded-Proto"
    val protoHeader = clientRequest.getScheme()
    proxyRequest.header(protoHeaderName, protoHeader)

  }

}
