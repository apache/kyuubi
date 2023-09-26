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

package org.apache.kyuubi.zookeeper

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.shaded.curator.framework.CuratorFrameworkFactory
import org.apache.kyuubi.shaded.curator.framework.imps.CuratorFrameworkState
import org.apache.kyuubi.shaded.curator.retry.ExponentialBackoffRetry
import org.apache.kyuubi.zookeeper.ZookeeperConf.{ZK_CLIENT_PORT, ZK_CLIENT_PORT_ADDRESS}

class EmbeddedZookeeperSuite extends KyuubiFunSuite {
  private var zkServer: EmbeddedZookeeper = _

  override def afterEach(): Unit = {
    if (zkServer != null) zkServer.stop()
    super.afterEach()
  }

  test("connect test with embedded zookeeper") {
    zkServer = new EmbeddedZookeeper()
    intercept[AssertionError](zkServer.getConnectString)
    zkServer.initialize(KyuubiConf().set(ZookeeperConf.ZK_CLIENT_PORT, 0))
    zkServer.start()

    val zkClient = CuratorFrameworkFactory.builder()
      .connectString(zkServer.getConnectString)
      .sessionTimeoutMs(5000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build
    zkClient.start()

    assert(zkClient.getState === CuratorFrameworkState.STARTED)
    assert(zkClient.getZookeeperClient.blockUntilConnectedOrTimedOut())
  }

  test("use zookeeper.embedded.client.port.address cover default hostname") {
    zkServer = new EmbeddedZookeeper()
    // cover default hostname
    var conf = KyuubiConf()
      .set(ZK_CLIENT_PORT, 0)
      .set(ZK_CLIENT_PORT_ADDRESS, "localhost")
    zkServer.initialize(conf)
    assert(zkServer.getConnectString.contains("localhost"))
    zkServer = new EmbeddedZookeeper()
    conf = KyuubiConf()
      .set(ZK_CLIENT_PORT, 0)
      .set(ZK_CLIENT_PORT_ADDRESS, "127.0.0.1")
    zkServer.initialize(conf)
    assert(zkServer.getConnectString.contains("127.0.0.1"))
  }
}
