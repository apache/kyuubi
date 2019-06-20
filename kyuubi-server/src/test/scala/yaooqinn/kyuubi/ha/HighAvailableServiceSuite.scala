/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ha

import java.io.{File, IOException}

import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooDefs}
import org.apache.zookeeper.KeeperException.ConnectionLossException
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.scalatest.{BeforeAndAfterEach, Matchers}

import yaooqinn.kyuubi.SecuredFunSuite
import yaooqinn.kyuubi.server.{FrontendService, KyuubiServer}
import yaooqinn.kyuubi.service.ServiceException

class HighAvailableServiceSuite extends SparkFunSuite
  with Matchers
  with SecuredFunSuite
  with ZookeeperFunSuite
  with BeforeAndAfterEach {

  private var server: KyuubiServer = _

  private var haService: HighAvailableService = _

  conf.set(KyuubiConf.HA_MODE.key, "failover")

  override def beforeEach(): Unit = {
    server = new KyuubiServer()
    haService = new HighAvailableService("test", server) {
      override def reset(): Unit = {}
    }
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.stop()
    }

    if (haService != null) {
      haService.stop()
    }

    super.afterEach()
  }

  test("ACL Provider") {
    val aclProvider = HighAvailableService.aclProvider
    aclProvider.getDefaultAcl should be(ZooDefs.Ids.OPEN_ACL_UNSAFE)
    aclProvider.getAclForPath("") should be(ZooDefs.Ids.OPEN_ACL_UNSAFE)

    tryWithSecurityEnabled {
      assert(aclProvider.getDefaultAcl.containsAll(ZooDefs.Ids.READ_ACL_UNSAFE))
      assert(aclProvider.getDefaultAcl.containsAll(ZooDefs.Ids.CREATOR_ALL_ACL))
    }
  }

  test("New Zookeeper Client") {
    val client = HighAvailableService.newZookeeperClient(conf)
    client.getState should be(CuratorFrameworkState.STARTED)
  }

  test("Get Quorum Servers") {
    val conf = new SparkConf(true)
    val e = intercept[IllegalArgumentException](HighAvailableService.getQuorumServers(conf))
    e.getMessage should startWith(KyuubiConf.HA_ZOOKEEPER_QUORUM.key)
    val host1 = "127.0.0.1:1234"
    val host2 = "127.0.0.1"
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, host1)
    HighAvailableService.getQuorumServers(conf) should be(host1)
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, host1 + "," + host2)
    HighAvailableService.getQuorumServers(conf) should be(host1 + "," + host2 + ":2181")
    val port = "2180"
    conf.set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, port)
    HighAvailableService.getQuorumServers(conf) should be(host1 + "," + host2 + ":" + port)
  }

  test("get service instance uri") {
    val fe = new FrontendService(server.beService)
    intercept[ServiceException](HighAvailableService.getServerInstanceURI(null))
    fe.getServerIPAddress
    intercept[ServiceException](HighAvailableService.getServerInstanceURI(fe))

    fe.init(conf)
    HighAvailableService.getServerInstanceURI(fe) should startWith(
      fe.getServerIPAddress.getHostName)
  }

  test("set up zookeeper auth") {
    tryWithSecurityEnabled {
      val keytab = File.createTempFile("user", "keytab")
      val principal = KyuubiSparkUtil.getCurrentUserName + "/localhost@" + "yaooqinn"

      conf.set(KyuubiSparkUtil.KEYTAB, keytab.getCanonicalPath)
      conf.set(KyuubiSparkUtil.PRINCIPAL, principal)

      HighAvailableService.setUpZooKeeperAuth(conf)

      conf.set(KyuubiSparkUtil.KEYTAB, keytab.getName)

      val e = intercept[IOException](HighAvailableService.setUpZooKeeperAuth(conf))

      e.getMessage should startWith(KyuubiSparkUtil.KEYTAB)
    }
  }

  test("init") {
    // connect to right zk
    haService.init(conf)

    // connect to wrong zk
    val confClone = conf.clone()
      .set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, connectString.split(":")(0))
      .set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, "2000")
    val e1 = intercept[ServiceException](haService.init(confClone))
    e1.getMessage should startWith("Unable to create Kyuubi namespace")
    assert(e1.getCause.isInstanceOf[ConnectionLossException])
  }

  test("deregister watcher") {

    val ha = new HighAvailableService("ha", server) { self =>
      override def reset(): Unit = {}
    }

    import Watcher.Event.EventType._

    val watcher = new ha.DeRegisterWatcher
    val nodeDel = new WatchedEvent(NodeDeleted, KeeperState.Expired, "")
    watcher.process(nodeDel)
    watcher.process(new WatchedEvent(NodeCreated, KeeperState.Expired, ""))
  }
}