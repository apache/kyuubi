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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.EngineScope.EngineScope

class EngineAppNameSuite extends KyuubiFunSuite {

  private val kyuubiConf: KyuubiConf = KyuubiConf()
  private val zkNamespace: String = "kyuubi"
  private val serverHost: String = "kentyao.org"
  private val serverPort: Int = 10001
  private val user: String = "hive"
  private val handle: String = "a9938028-667d-4006-993e-0bdb5a14ae91"

  override def beforeAll(): Unit = {
    super.beforeAll()
    kyuubiConf.set(KyuubiConf.FRONTEND_BIND_HOST, serverHost)
    kyuubiConf.set(KyuubiConf.FRONTEND_BIND_PORT, serverPort)
  }


  test("SparkSQLEngineAppName") {

    // SESSION SCOPE
    val sessionScopeAppName = "kyuubi_kentyao.org_10001_[S]hive_a9938028-667d-4006-993e-0bdb5a14ae91"
    val sessionScopeZkPath = "/kyuubi/sessions/a9938028-667d-4006-993e-0bdb5a14ae91"
    checkAppNameAndZkPath(EngineScope.SESSION, sessionScopeAppName, sessionScopeZkPath)

    // USER SCOPE
    val userScopeAppName = "kyuubi_kentyao.org_10001_[U]hive_a9938028-667d-4006-993e-0bdb5a14ae91"
    val userScopeZkPath = "/kyuubi/users/hive"
    checkAppNameAndZkPath(EngineScope.USER, userScopeAppName, userScopeZkPath)

    // GROUP SCOPE
    val groupScopeAppName = "kyuubi_kentyao.org_10001_[G]hive_a9938028-667d-4006-993e-0bdb5a14ae91"
    val groupScopeZkPath = "/kyuubi/groups/default"
    checkAppNameAndZkPath(EngineScope.GROUP, groupScopeAppName, groupScopeZkPath)

    // SERVER SCOPE
    val serverScopeAppName = "kyuubi_kentyao.org_10001_[K]hive_a9938028-667d-4006-993e-0bdb5a14ae91"
    val serverScopeZkPath = "/kyuubi/servers/kentyao.org:10001"
    checkAppNameAndZkPath(EngineScope.SERVER, serverScopeAppName, serverScopeZkPath)

  }

  private def checkAppNameAndZkPath(scope: EngineScope,
      expectAppName: String, expectZkPath: String): Unit = {
    kyuubiConf.set(KyuubiConf.ENGINE_SCOPE, scope.toString)
    val engine = EngineAppName(user, handle, kyuubiConf.getUserDefaults(user))
    assert(engine.generateAppName() === expectAppName)
    assert(engine.makeZkPath(zkNamespace) === expectZkPath)
    val zkPath = EngineAppName.parseAppName(expectAppName, kyuubiConf).makeZkPath(zkNamespace)
    assert(zkPath === expectZkPath)
  }


}
