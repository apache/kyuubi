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

package org.apache.kyuubi.ha.client

import java.nio.charset.StandardCharsets
import java.util.Base64

case class DataAgentSessionRoute(engineSpace: String, engineRefId: String, user: String)

object DataAgentSessionRoute {
  private val ROOT_SUFFIX = "DATA_AGENT_sessions"

  def root(serverSpace: String): String = s"${serverSpace}_$ROOT_SUFFIX"

  def path(serverSpace: String, sessionId: String): String =
    DiscoveryPaths.makePath(root(serverSpace), sessionId)

  def encode(route: DataAgentSessionRoute): Array[Byte] = {
    Seq(route.engineSpace, route.engineRefId, route.user)
      .map(value =>
        Base64.getUrlEncoder.withoutPadding()
          .encodeToString(value.getBytes(StandardCharsets.UTF_8)))
      .mkString("\n")
      .getBytes(StandardCharsets.UTF_8)
  }

  def decode(bytes: Array[Byte]): DataAgentSessionRoute = {
    val values = new String(bytes, StandardCharsets.UTF_8).split("\n", -1)
    require(values.length == 3, "Invalid Data Agent session route")
    def decodeValue(value: String): String =
      new String(Base64.getUrlDecoder.decode(value), StandardCharsets.UTF_8)
    DataAgentSessionRoute(decodeValue(values(0)), decodeValue(values(1)), decodeValue(values(2)))
  }
}
