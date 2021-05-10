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

class EngineNameSuite extends KyuubiFunSuite {
  import ShareLevel._

  test(s"${CONNECTION} shared level engine name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    val user = Utils.currentUser
    Seq(Some("suffix"), None).foreach { maybeSubDomain =>
      val appName = EngineName(CONNECTION, user, id, maybeSubDomain)
      assert(appName.getEngineSpace("kyuubi") ===
        ZKPaths.makePath(s"kyuubi_$CONNECTION", user, id.identifier.toString))
      assert(appName.defaultEngineName ===  s"kyuubi_${CONNECTION}_${user}_${id.identifier}")
      intercept[AssertionError](appName.getZkLockPath("kyuubi"))
    }
  }

  test(s"${USER} shared level engine name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    val user = Utils.currentUser
    val appName = EngineName(USER, user, id, None)
    assert(appName.getEngineSpace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$USER", user))
    assert(appName.defaultEngineName ===  s"kyuubi_${USER}_${user}_${id.identifier}")
    assert(appName.getZkLockPath("kyuubi") === s"/kyuubi_${USER}/lock/$user")

    val appName2 = EngineName(USER, user, id, Some("abc"))
    assert(appName2.getEngineSpace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$USER", user, "abc"))
    assert(appName2.defaultEngineName ===  s"kyuubi_${USER}_${user}_abc_${id.identifier}")
    assert(appName2.getZkLockPath("kyuubi") === s"/kyuubi_${USER}/lock/$user/abc")
  }

  test(s"${SERVER} shared level engine name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    val user = Utils.currentUser
    val appName = EngineName(SERVER, user, id, None)
    assert(appName.getEngineSpace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$SERVER", user))
    assert(appName.defaultEngineName ===  s"kyuubi_${SERVER}_${user}_${id.identifier}")
    assert(appName.getZkLockPath("kyuubi") === s"/kyuubi_${SERVER}/lock/$user")


    val appName2 = EngineName(SERVER, user, id, Some("abc"))
    assert(appName2.getEngineSpace("kyuubi") ===
      ZKPaths.makePath(s"kyuubi_$SERVER", user, "abc"))
    assert(appName2.defaultEngineName ===  s"kyuubi_${SERVER}_${user}_abc_${id.identifier}")
    assert(appName2.getZkLockPath("kyuubi") === s"/kyuubi_${SERVER}/lock/$user/abc")
  }
}
