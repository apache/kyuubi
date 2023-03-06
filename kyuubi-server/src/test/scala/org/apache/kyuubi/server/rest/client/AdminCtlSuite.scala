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

package org.apache.kyuubi.server.rest.client

import java.util.UUID

import org.apache.kyuubi.{KYUUBI_VERSION, RestClientTestHelper}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.{CtlConf, TestPrematureExit}
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryPaths

class AdminCtlSuite extends RestClientTestHelper with TestPrematureExit {
  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty(CtlConf.CTL_REST_CLIENT_BASE_URL.key, baseUri.toString)
    System.setProperty(CtlConf.CTL_REST_CLIENT_SPNEGO_HOST.key, "localhost")
  }

  override def afterAll(): Unit = {
    System.clearProperty(CtlConf.CTL_REST_CLIENT_BASE_URL.key)
    System.clearProperty(CtlConf.CTL_REST_CLIENT_SPNEGO_HOST.key)
    System.clearProperty(CtlConf.CTL_REST_CLIENT_AUTH_SCHEMA.key)
    super.afterAll()
  }

  test("refresh config - hadoop conf") {
    val args = Array("refresh", "config", "hadoopConf", "--authSchema", "spnego")
    testPrematureExitForAdminControlCli(
      args,
      s"Refresh the hadoop conf for ${fe.connectionUrl} successfully.")
  }

  test("engine list/delete operation") {
    val id = UUID.randomUUID().toString
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "kyuubi_test")
    conf.set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 180000L)
    conf.set(KyuubiConf.AUTHENTICATION_METHOD, Seq("LDAP", "CUSTOM"))
    val user = ldapUser
    val engine = new EngineRef(conf.clone, user, "grp", id, null)

    val engineSpace = DiscoveryPaths.makePath(
      s"kyuubi_test_${KYUUBI_VERSION}_USER_SPARK_SQL",
      user,
      "default")

    withDiscoveryClient(conf) { client =>
      engine.getOrCreate(client)

    }

    var args = Array(
      "list",
      "engine",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    testPrematureExitForAdminControlCli(
      args,
      "Engine Node List (total 1)")

    args = Array(
      "delete",
      "engine",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    testPrematureExitForAdminControlCli(
      args,
      s"Engine ${engineSpace} is deleted successfully.")

    args = Array(
      "list",
      "engine",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    testPrematureExitForAdminControlCli(
      args,
      "Engine Node List (total 0)")
  }
}
