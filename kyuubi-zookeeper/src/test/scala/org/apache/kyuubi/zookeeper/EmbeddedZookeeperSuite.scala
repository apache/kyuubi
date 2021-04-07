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
import org.apache.kyuubi.service.ServiceState._
import org.apache.kyuubi.shade.curator.framework.CuratorFrameworkFactory
import org.apache.kyuubi.shade.curator.framework.imps.CuratorFrameworkState
import org.apache.kyuubi.shade.curator.retry.ExponentialBackoffRetry

class EmbeddedZookeeperSuite extends KyuubiFunSuite {

  test("embedded zookeeper server") {
    val zkServer = new EmbeddedZookeeper()
    assert(zkServer.getConf == null)
    assert(zkServer.getName === zkServer.getClass.getSimpleName)
    assert(zkServer.getServiceState === LATENT)
    val conf = KyuubiConf()
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    zkServer.stop() // only for test coverage
    zkServer.initialize(conf)
    assert(zkServer.getConf === conf)
    assert(zkServer.getServiceState === INITIALIZED)
    assert(zkServer.getStartTime === 0)
    zkServer.start()
    assert(zkServer.getServiceState === STARTED)
    assert(zkServer.getStartTime !== 0)
    zkServer.stop()
    assert(zkServer.getServiceState === STOPPED)
  }

  test("connect test with embedded zookeeper") {
    val zkServer = new EmbeddedZookeeper()
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
}
