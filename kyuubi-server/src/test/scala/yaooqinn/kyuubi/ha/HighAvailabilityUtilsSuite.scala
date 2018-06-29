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

import org.apache.curator.test.TestingServer
import org.apache.spark.{KyuubiConf, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf.HA_ZOOKEEPER_CONNECTION_MAX_RETRIES
import org.apache.zookeeper.KeeperException.ConnectionLossException
import org.scalatest.{BeforeAndAfterEach, ConfigMap}

import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.ServiceException

class HighAvailabilityUtilsSuite extends SparkFunSuite with BeforeAndAfterEach {

  var zkServer: TestingServer = _
  var connectString: String = _
  val conf = new SparkConf(loadDefaults = true)
  KyuubiServer.setupCommonConfig(conf)
  conf.set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
  var server: KyuubiServer = _

  override def beforeAll(configMap: ConfigMap): Unit = {
    zkServer = new TestingServer(2181, true)
    connectString = zkServer.getConnectString
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, connectString)
    conf.set(KyuubiConf.HA_ZOOKEEPER_ZNODE_CREATION_TIMEOUT.key, "1s")
    conf.set(KyuubiConf.HA_ZOOKEEPER_SESSION_TIMEOUT.key, "3s")
    conf.set(HA_ZOOKEEPER_CONNECTION_MAX_RETRIES.key, "1")
  }

  override def afterAll(): Unit = {
    zkServer.stop()
  }

  override def afterEach(): Unit = {
    server.stop()
  }

  test("Add Server Instance To ZooKeeper with host:port") {
    server = new KyuubiServer()
    server.init(conf)
    server.start()
    HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
  }

  test("Add Server Instance To ZooKeeper with wrong port") {
    server = new KyuubiServer()
    conf.set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, "2000")
    server.init(conf)
    server.start()
    HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
  }

  test("Add Server Instance To ZooKeeper with host and port") {
    server = new KyuubiServer()
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, connectString.split(":")(0))
    conf.set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, "2181")
    server.init(conf)
    server.start()
    HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
  }

  test("Add Server Instance To ZooKeeper with host and wrong port") {
    server = new KyuubiServer()
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, connectString.split(":")(0))
    conf.set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, "2000")
    server.init(conf)
    server.start()
    val e = intercept[ServiceException](HighAvailabilityUtils.addServerInstanceToZooKeeper(server))
    assert(e.getCause.isInstanceOf[ConnectionLossException])
  }

  test("Add Server Instance To ZooKeeper with wrong host and right port") {
    server = new KyuubiServer()
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, connectString.split(":")(0) + "1")
    conf.set(KyuubiConf.HA_ZOOKEEPER_CLIENT_PORT.key, "2181")
    server.init(conf)
    server.start()
    val e = intercept[ServiceException](HighAvailabilityUtils.addServerInstanceToZooKeeper(server))
    assert(e.getCause.isInstanceOf[ConnectionLossException])
  }

  test("Is Support Dynamic Service Discovery") {
    val conf = new SparkConf(loadDefaults = true)
    assert(!HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf))
    KyuubiServer.setupCommonConfig(conf)
    assert(!HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf))
    conf.set(KyuubiConf.HA_ENABLED.key, "true")
    assert(!HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf))
    conf.set(KyuubiConf.HA_ZOOKEEPER_QUORUM.key, "localhost")
    assert(HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf))
  }
}
