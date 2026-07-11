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

import org.apache.kyuubi.KyuubiFunSuite

class DataAgentSessionRouteSuite extends KyuubiFunSuite {

  test("encode and decode route") {
    val route = DataAgentSessionRoute(
      "kyuubi_1.12.0_USER_DATA_AGENT/alice/ds-1234",
      "engine-ref-id",
      "alice@example.com")
    assert(DataAgentSessionRoute.decode(DataAgentSessionRoute.encode(route)) === route)
  }

  test("encode and decode route with unicode and delimiters") {
    val namespace = Seq(0x547D, 0x540D, 0x7A7A, 0x95F4).map(_.toChar).mkString
    val user = Seq(0x7528, 0x6237).map(_.toChar).mkString
    val route = DataAgentSessionRoute(
      s"$namespace/with spaces",
      "ref:id/with-delimiters",
      s"$user+data@example.com")
    assert(DataAgentSessionRoute.decode(DataAgentSessionRoute.encode(route)) === route)
  }

  test("build route path") {
    assert(DataAgentSessionRoute.path("kyuubi", "session-id") ===
      "/kyuubi_DATA_AGENT_sessions/session-id")
  }

  test("reject malformed route") {
    intercept[IllegalArgumentException] {
      DataAgentSessionRoute.decode("not-a-route".getBytes)
    }
  }

  test("reject route with malformed base64 value") {
    intercept[IllegalArgumentException] {
      DataAgentSessionRoute.decode("%%%%\nZW5naW5l\ndXNlcg".getBytes)
    }
  }
}
