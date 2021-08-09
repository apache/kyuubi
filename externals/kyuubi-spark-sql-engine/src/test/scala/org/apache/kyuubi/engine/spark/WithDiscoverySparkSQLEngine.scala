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
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ZK_ACL_ENABLED, HA_ZK_NAMESPACE, HA_ZK_QUORUM}
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

trait WithDiscoverySparkSQLEngine extends WithSparkSQLEngine {
  private var zkServer: EmbeddedZookeeper = _
  def namespace: String
  override def withKyuubiConf: Map[String, String] = {
    assert(zkServer != null)
    Map(HA_ZK_QUORUM.key -> zkServer.getConnectString,
      HA_ZK_ACL_ENABLED.key -> "false",
      HA_ZK_NAMESPACE.key -> namespace)
  }

  override def beforeAll(): Unit = {
    zkServer = new EmbeddedZookeeper()
    val zkData = Utils.createTempDir()
    val tmpConf = KyuubiConf()
    tmpConf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    tmpConf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
    zkServer.initialize(tmpConf)
    zkServer.start()
  }

  override def afterAll(): Unit = {
    if (zkServer != null) {
      zkServer.stop()
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    startSparkEngine()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    stopSparkEngine()
  }

  def withZkClient(f: CuratorFramework => Unit): Unit = {
    ServiceDiscovery.withZkClient(kyuubiConf)(f)
  }

  protected def getDiscoveryConnectionString: String = {
    if (zkServer == null) {
      ""
    } else {
      zkServer.getConnectString
    }
  }
}
