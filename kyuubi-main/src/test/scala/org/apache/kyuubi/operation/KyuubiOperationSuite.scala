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

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.server.EmbeddedZkServer
import org.apache.kyuubi.server.KyuubiServer

abstract class KyuubiOperationSuite extends JDBCTests {

  protected val conf: KyuubiConf

  private var zkServer: EmbeddedZkServer = _
  private var server: KyuubiServer = _

  override def beforeAll(): Unit = {
    zkServer = new EmbeddedZkServer()
    conf.set(KyuubiConf.EMBEDDED_ZK_PORT, -1)
    val zkData = Utils.createTempDir()
    conf.set(KyuubiConf.EMBEDDED_ZK_TEMP_DIR, zkData.toString)
    zkServer.initialize(conf)
    zkServer.start()

    conf.set("spark.ui.enabled", "false")
    conf.set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    conf.set(KyuubiConf.ENGINE_CHECK_INTERVAL, 4000L)
    conf.set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 10000L)
    conf.set(HA_ZK_QUORUM, zkServer.getConnectString)
    conf.set(HA_ZK_ACL_ENABLED, false)

    server = KyuubiServer.startServer(conf)
    super.beforeAll()
  }

  override def afterAll(): Unit = {

    if (server != null) {
      server.stop()
      server = null
    }

    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
    super.afterAll()
  }

  override protected def jdbcUrl: String = s"jdbc:hive2://${server.connectionUrl}/;"
}
