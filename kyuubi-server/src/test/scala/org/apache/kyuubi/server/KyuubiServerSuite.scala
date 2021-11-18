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

package org.apache.kyuubi.server

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider
import org.apache.kyuubi.service.ServiceState._
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class KyuubiServerSuite extends KyuubiFunSuite {

  private var zkServer: EmbeddedZookeeper = _
  private var server: KyuubiServer = _

  override def beforeAll(): Unit = {
    val conf = KyuubiConf()
    zkServer = new EmbeddedZookeeper()
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    val zkData = Utils.createTempDir()
    conf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
    zkServer.initialize(conf)
    zkServer.start()
    super.beforeAll()
    Thread.sleep(1000)
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

  test("kyuubi server basic") {
    val server = new KyuubiServer()
    server.stop()
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    assert(server.getServices.isEmpty)
    assert(server.getServiceState === LATENT)
    assert(server.getConf === null)
    // server.frontendServices should access after initializing
    assertThrows[NullPointerException](server.frontendServices)

    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    val backendServices = server.getServices.filter(_.isInstanceOf[KyuubiBackendService])
    assert(backendServices.size == 1)
    val backendService = backendServices(0).asInstanceOf[KyuubiBackendService]
    assert(backendService.getServiceState == INITIALIZED)
    assert(backendService.getServices.forall(_.getServiceState === INITIALIZED))
    assert(server.frontendServices.head.connectionUrl.split(":").length === 2)
    assert(server.getConf === conf)
    assert(server.getStartTime === 0)
    server.stop()


    server.start()
    assert(server.getServiceState === STARTED)
    assert(backendService.getServiceState == STARTED)
    assert(backendService.getServices.forall(_.getServiceState === STARTED))
    assert(server.getStartTime !== 0)

    server.stop()
    assert(server.getServiceState === STOPPED)
    assert(backendService.getServiceState == STOPPED)
    assert(backendService.getServices.forall(_.getServiceState === STOPPED))
    server.stop()
  }

  test("invalid port") {
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 100)
    val e = intercept[KyuubiException](new KyuubiServer().initialize(conf))
    assert(e.getMessage.contains("Failed to initialize frontend service"))
    assert(e.getCause.getMessage === "Invalid Port number")
  }

  test("invalid zookeeper quorum") {
    val conf = KyuubiConf()
    val quorum1 = "localhost:2181/lake,localhost:2182/lake"
    conf.set(HighAvailabilityConf.HA_ZK_QUORUM, quorum1)
    val exp1 = intercept[IllegalArgumentException](KyuubiServer.startServer(conf))
    assert(exp1.getMessage === s"Illegal zookeeper quorum '$quorum1', " +
      s"the chroot path started with / is only allowed at the end!")

    val quorum2 = "localhost:2181/lake,localhost:2182"
    conf.set(HighAvailabilityConf.HA_ZK_QUORUM, quorum2)
    val exp2 = intercept[IllegalArgumentException](KyuubiServer.startServer(conf))
    assert(exp2.getMessage === s"Illegal zookeeper quorum '$quorum2', " +
      s"the chroot path started with / is only allowed at the end!")
  }

  test("kyuubi server starts with chroot") {
    val conf = KyuubiConf()
    val zkConnection = zkServer.getConnectString
    val chrootPath = "/lake"
    conf.set(HighAvailabilityConf.HA_ZK_QUORUM, zkConnection)
    // chroot path does not exist before server start
    ZooKeeperClientProvider.withZkClient(conf) { client =>
      assert(client.checkExists().forPath(chrootPath) == null)
    }

    val zkWithChroot = zkConnection + chrootPath
    val chrootConf = conf.clone.set(HighAvailabilityConf.HA_ZK_QUORUM, zkWithChroot)
    server = KyuubiServer.startServer(chrootConf)
    // chroot path exists after server started
    ZooKeeperClientProvider.withZkClient(conf) { client =>
      assert(client.checkExists().forPath(chrootPath) != null)
    }
  }
}
