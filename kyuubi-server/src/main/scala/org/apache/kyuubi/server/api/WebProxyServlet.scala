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

import scala.collection.mutable

import org.apache.http.HttpHost
import org.eclipse.jetty.proxy.ProxyServlet

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

private[api] class WebProxyServlet(conf: KyuubiConf) extends ProxyServlet with Logging {
  override def rewriteTarget(request: HttpServletRequest): String = {
    var targetUrl = "/no-ui-error"
    val requestUrl = request.getRequestURI
    logger.info("requestUrl is {}", requestUrl)
    val url = request.getParameterMap.get("url")
    logger.info("url is {}", url)
    if (url != null && url.length > 0) {
      val ipAddress = url(0).split(":")(1)
      val port = url(0).split(":")(2).toInt
      request.setAttribute(
        this.getClass.getSimpleName,
        new HttpHost(ipAddress, port, "http"))
      targetUrl = new mutable.StringBuilder().append("/jobs/").toString()
      logger.info("ui -> http://{}:{}{}", ipAddress, port.toString, targetUrl)
    }
    targetUrl
  }
}
