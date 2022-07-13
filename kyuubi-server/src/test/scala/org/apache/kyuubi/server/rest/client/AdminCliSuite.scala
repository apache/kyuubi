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

import org.apache.kyuubi.RestClientTestHelper
import org.apache.kyuubi.ctl.{CtlConf, TestPrematureExit}

class AdminCliSuite extends RestClientTestHelper with TestPrematureExit {
  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty(CtlConf.CTL_REST_CLIENT_BASE_URL.key, baseUri.toString)
    System.setProperty(CtlConf.CTL_REST_CLIENT_SPNEGO_HOST.key, "localhost")
    System.setProperty(CtlConf.CTL_REST_CLIENT_AUTH_SCHEMA.key, "spnego")
  }

  override def afterAll(): Unit = {
    System.clearProperty(CtlConf.CTL_REST_CLIENT_BASE_URL.key)
    System.clearProperty(CtlConf.CTL_REST_CLIENT_SPNEGO_HOST.key)
    System.clearProperty(CtlConf.CTL_REST_CLIENT_AUTH_SCHEMA.key)
    super.afterAll()
  }

  test("refresh kyuubi server hadoop conf") {
    fe.connectionUrl
    val args = Array("administer", "server", "refreshHadoopConf")
    testPrematureExitForControlCli(
      args,
      s"Refresh the hadoop conf for ${fe.connectionUrl} successfully.")
  }
}
