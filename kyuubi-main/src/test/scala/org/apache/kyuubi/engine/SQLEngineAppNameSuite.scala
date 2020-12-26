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

package org.apache.kyuubi.engine

import org.apache.curator.utils.ZKPaths
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.session.SessionHandle

class SQLEngineAppNameSuite extends KyuubiFunSuite {
  import ShareLevel._

  test("SESSION_LEVEL sql engine app name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10).identifier
    val user = Utils.currentUser
    val appName = SQLEngineAppName(CONNECTION, user, id.toString)
    assert(appName.getZkNamespace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$CONNECTION", user, id.toString))
    assert(appName.toString ===  s"kyuubi_${CONNECTION}_${user}_$id")
  }

  test("USER_LEVEL sql engine app name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10).identifier
    val user = Utils.currentUser
    val appName = SQLEngineAppName(USER, user, id.toString)
    assert(appName.getZkNamespace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$USER", user))
    assert(appName.toString ===  s"kyuubi_${USER}_${user}_$id")
  }

  test("QUEUE_LEVEL sql engine app name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10).identifier
    val user = Utils.currentUser
    val appName = SQLEngineAppName(GROUP, user, id.toString)
    assert(appName.getZkNamespace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$GROUP", user))
    assert(appName.toString ===  s"kyuubi_${GROUP}_${user}_$id")
  }

  test("SERVER_LEVEL sql engine app name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10).identifier
    val user = Utils.currentUser
    val appName = SQLEngineAppName(SERVER, user, id.toString)
    assert(appName.getZkNamespace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$SERVER", user))
    assert(appName.toString ===  s"kyuubi_${SERVER}_${user}_$id")
  }
}
