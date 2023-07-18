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

package org.apache.kyuubi.operation

import java.nio.charset.StandardCharsets

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.DiscoveryClientProvider
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

class KyuubiOperationWithEngineSecuritySuite extends WithKyuubiServer with HiveJDBCTestHelper {
  import DiscoveryClientProvider._

  override protected def jdbcUrl: String = getJdbcUrl

  private val engineSecretNode = "/SECRET"
  private val engineSecret = "_ENGINE_SECRET_"

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.ENGINE_SECURITY_ENABLED, false)
      .set(KyuubiConf.ENGINE_SECURITY_SECRET_PROVIDER, "zookeeper")
      .set(HighAvailabilityConf.HA_ZK_ENGINE_SECURE_SECRET_NODE, engineSecretNode)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    withDiscoveryClient(conf) { discoveryClient =>
      discoveryClient.create(engineSecretNode, "PERSISTENT", false)
      discoveryClient.startSecretNode("PERSISTENT", engineSecretNode, engineSecret)
      val expected = engineSecret.getBytes(StandardCharsets.UTF_8)
      assert(discoveryClient.getData(engineSecretNode) === expected)
    }

    conf.set(KyuubiConf.ENGINE_SECURITY_ENABLED, true)
    InternalSecurityAccessor.initialize(conf, true)
  }

  test("engine security") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery(s"set spark.${KyuubiConf.ENGINE_SECURITY_ENABLED.key}")
      assert(rs.next())
      assert(rs.getString(2).contains("true"))
    }
  }
}
