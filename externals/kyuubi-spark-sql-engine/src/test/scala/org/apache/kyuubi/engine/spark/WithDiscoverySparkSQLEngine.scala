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

package org.apache.kyuubi.engine.spark

import org.apache.curator.framework.CuratorFramework

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{EMBEDDED_ZK_PORT, EMBEDDED_ZK_TEMP_DIR}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ZK_ACL_ENABLED, HA_ZK_NAMESPACE, HA_ZK_QUORUM}
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.ha.server.EmbeddedZkServer

trait WithDiscoverySparkSQLEngine extends WithSparkSQLEngine {
  private var zkServer: EmbeddedZkServer = _
  def kyuubiConf: KyuubiConf = SparkSQLEngine.kyuubiConf
  def namespace: String

  override protected def beforeEach(): Unit = {
    zkServer = new EmbeddedZkServer()
    val zkData = Utils.createTempDir()
    kyuubiConf.set(EMBEDDED_ZK_PORT, -1)
    kyuubiConf.set(EMBEDDED_ZK_TEMP_DIR, zkData.toString)
    zkServer.initialize(kyuubiConf)
    zkServer.start()
    kyuubiConf.set(HA_ZK_QUORUM, zkServer.getConnectString)
    kyuubiConf.set(HA_ZK_ACL_ENABLED, false)
    kyuubiConf.set(HA_ZK_NAMESPACE, namespace)
    super.beforeEach()
    startSparkEngine()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    stopSparkEngine()

    if (zkServer != null) {
      zkServer.stop()
    }
  }

  def withZkClient(f: CuratorFramework => Unit): Unit = {
    val zkClient = ServiceDiscovery.startZookeeperClient(kyuubiConf)
    try {
      f(zkClient)
    } finally {
      zkClient.close()
    }
  }

  protected def getDiscoveryConnectionString: String = {
    if (zkServer == null) {
      ""
    } else {
      zkServer.getConnectString
    }
  }
}
