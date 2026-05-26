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

import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_URL
import org.apache.kyuubi.ha.client.DiscoveryClient

class EngineUIProxyServletSuite extends KyuubiFunSuite {

  test("rewrite target only for registered engine UI") {
    val servlet = new EngineUIProxyServlet(true, () => Set("engine-host" -> 4040))

    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4040")) ===
      "http://engine-host:4040/jobs/")
    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4040/sql/", "id=1")) ===
      "http://engine-host:4040/sql/?id=1")
    assert(servlet.rewriteTarget(request("/engine-ui/unknown-host:4040")) === null)
  }

  test("rewrite target without filter for compatibility") {
    val servlet = new EngineUIProxyServlet(false, () => Set.empty)

    assert(servlet.rewriteTarget(request("/engine-ui/unknown-host:4040")) ===
      "http://unknown-host:4040/jobs/")
  }

  test("normalize registered engine URLs") {
    assert(EngineUIProxyServlet.normalizeEngineUrl("engine-host:4040") ===
      Some("engine-host" -> 4040))
    assert(EngineUIProxyServlet.normalizeEngineUrl("http://ENGINE-HOST:4040/jobs/") ===
      Some("engine-host" -> 4040))
    assert(EngineUIProxyServlet.normalizeEngineUrl("https://engine-host:4040") ===
      Some("engine-host" -> 4040))
    assert(EngineUIProxyServlet.normalizeEngineUrl("engine-host") === None)
  }

  test("registered engine URLs are collected from engine namespaces") {
    val discoveryClient = mock[DiscoveryClient]
    val engineNamespace = "/kyuubi_1.12.0-SNAPSHOT_USER_SPARK_SQL"
    val engineNode = s"serverUri=engine-host:10010;$KYUUBI_ENGINE_URL=engine-host:4040"
    when(discoveryClient.getChildren("/")).thenReturn(List(
      "kyuubi",
      engineNamespace.stripPrefix("/")))
    when(discoveryClient.getChildren(engineNamespace)).thenReturn(List("anonymous"))
    when(discoveryClient.getChildren(s"$engineNamespace/anonymous")).thenReturn(List("default"))
    when(discoveryClient.getChildren(s"$engineNamespace/anonymous/default")).thenReturn(List(
      engineNode))

    assert(EngineUIProxyServlet.registeredEngineUrls(discoveryClient, "kyuubi") ===
      Set("engine-host" -> 4040))
  }

  private def request(uri: String, queryString: String = null): HttpServletRequest = {
    val request = mock[HttpServletRequest]
    when(request.getRequestURL).thenReturn(new StringBuffer(s"http://kyuubi$uri"))
    when(request.getRequestURI).thenReturn(uri)
    when(request.getQueryString).thenReturn(queryString)
    request
  }
}
