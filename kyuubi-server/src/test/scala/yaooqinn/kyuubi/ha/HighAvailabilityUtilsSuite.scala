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

import com.google.common.io.Files
import org.apache.curator.test.TestingServer
import org.apache.spark.{KyuubiConf, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.apache.zookeeper.KeeperException.ConnectionLossException
import org.scalatest.BeforeAndAfterEach

import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.ServiceException

class HighAvailabilityUtilsSuite extends SparkFunSuite with BeforeAndAfterEach {

  var zkServer: TestingServer = _
  var connectString: String = _
  val conf = new SparkConf(loadDefaults = true)
  KyuubiServer.setupCommonConfig(conf)
  conf.set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
  var server: KyuubiServer = _

  override def beforeAll(): Unit = {
    zkServer = new TestingServer(2181, Files.createTempDir(), true)
    connectString = zkServer.getConnectString
    conf.set(HA_ZOOKEEPER_QUORUM.key, connectString)
    conf.set(HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME.key, "100ms")
    conf.set(HA_ZOOKEEPER_ZNODE_CREATION_TIMEOUT.key, "1s")
    conf.set(HA_ZOOKEEPER_SESSION_TIMEOUT.key, "15s")
    conf.set(HA_ZOOKEEPER_CONNECTION_MAX_RETRIES.key, "0")
  }

  override def afterAll(): Unit = {
    zkServer.stop()
    System.clearProperty(HA_ZOOKEEPER_QUORUM.key)
    System.clearProperty(HA_ENABLED.key)
  }

  override def afterEach(): Unit = {
    server.stop()
  }

  test("Add Server Instance To ZooKeeper with host:port") {
    val conf1 = conf.clone
    server = new KyuubiServer()
    server.init(conf1)
    server.start()
    HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
  }

  test("Add Server Instance To ZooKeeper with wrong port") {
    server = new KyuubiServer()
    val conf1 = conf.clone().set(HA_ZOOKEEPER_CLIENT_PORT.key, "2000")
    server.init(conf1)
    server.start()
    HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
  }

  test("Add Server Instance To ZooKeeper with host and port") {
    server = new KyuubiServer()
    val conf1 = conf.clone().set(HA_ZOOKEEPER_QUORUM.key, connectString.split(":")(0))
      .set(HA_ZOOKEEPER_CLIENT_PORT.key, "2181")
    server.init(conf1)
    server.start()
    HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
  }

  test("Add Server Instance To ZooKeeper with host and wrong port") {
    server = new KyuubiServer()
    val conf1 = conf.clone().set(HA_ZOOKEEPER_QUORUM.key, connectString.split(":")(0))
      .set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, "2000")
    server.init(conf1)
    server.start()
    val e = intercept[ServiceException](HighAvailabilityUtils.addServerInstanceToZooKeeper(server))
    assert(e.getCause.isInstanceOf[ConnectionLossException])
    assert(e.getMessage.contains("ZooKeeper is still unreachable"))
  }

  test("Add Server Instance To ZooKeeper with wrong host and right port") {
    server = new KyuubiServer()
    val conf1 = conf.clone().set(HA_ZOOKEEPER_QUORUM.key, "github.com")
      .set(HA_ZOOKEEPER_CLIENT_PORT.key, "2181")
    server.init(conf1)
    server.start()
    intercept[ServiceException](HighAvailabilityUtils.addServerInstanceToZooKeeper(server))
  }

  test("Is Support Dynamic Service Discovery") {
    val conf1 = new SparkConf(loadDefaults = true)
    assert(!HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf1))
    KyuubiServer.setupCommonConfig(conf1)
    assert(!HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf1))
    conf1.set(KyuubiConf.HA_ENABLED.key, "true")
    assert(!HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf1))
    conf1.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, "localhost")
    assert(HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf1))
  }

  test("start kyuubi server") {
    val conf1 = conf.clone().set(HA_ENABLED.key, "true")
    conf1.getAll.foreach { case (k, v) =>
      sys.props(k) = v
    }
    server = KyuubiServer.startKyuubiServer()
    val conf2 = server.getConf
    assert(conf2.get(HA_ZOOKEEPER_QUORUM.key) === connectString)
    assert(HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf1))
  }
}
