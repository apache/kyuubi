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

import java.io.IOException

import scala.collection.JavaConverters._

import org.apache.curator.framework.recipes.leader.LeaderLatchListener
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkFunSuite}
import org.scalatest.{BeforeAndAfterEach, Matchers}

import yaooqinn.kyuubi.SecuredFunSuite
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.State.{INITED, NOT_INITED, STARTED}

class FailoverServiceSuite extends SparkFunSuite
  with ZookeeperFunSuite
  with Matchers
  with SecuredFunSuite
  with BeforeAndAfterEach {

  private var server: KyuubiServer = _

  private var haService: HighAvailableService = _

  conf.set(KyuubiConf.HA_MODE.key, "failover")

  override def beforeEach(): Unit = {
    server = new KyuubiServer()
    haService = new FailoverService(server)
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

  test("Init") {
    haService.getConf should be(null)
    haService.getStartTime should be(0)
    haService.getName should be(classOf[FailoverService].getSimpleName)
    haService.getServiceState should be(NOT_INITED)

    haService.init(conf)
    haService.getConf should be(conf)
    haService.getStartTime should be(0)
    haService.getServiceState should be(INITED)

    tryWithSecurityEnabled {
      val e = intercept[IOException](haService.init(conf))
      e.getMessage should startWith(KyuubiSparkUtil.KEYTAB)
    }
  }

  test("Start") {
    server.init(conf)
    val e2 = intercept[NullPointerException](haService.start())
    e2.getMessage should be("client cannot be null")
    haService.init(conf)
    haService.start()
    haService.getServiceState should be(STARTED)
    haService.getStartTime should not be 0
    haService.asInstanceOf[LeaderLatchListener].isLeader()
    haService.asInstanceOf[LeaderLatchListener].notLeader()
  }

  test("Stop before init") {
    haService.stop()
  }

  test("Stop after init") {
    haService.init(conf)
    haService.stop()
  }

  test("Stop after start") {
    server.init(conf)
    haService.init(conf)
    haService.start()
    haService.stop()
  }

  test("gain leader ship") {
    server.init(conf)
    haService.init(conf)
    haService.start()
    Thread.sleep(5000)
    val list = zooKeeperClient.getChildren.forPath("/").asScala.toList
    assert(list.size === 3)
    val ns = "kyuubiserver"
    assert(list.contains(ns))
    assert(list.contains(ns + "-latch"))
    val l2 = zooKeeperClient.getChildren.forPath("/" + ns).asScala.toList
    assert(l2.size === 1)
    val l3 = zooKeeperClient.getChildren.forPath("/" + ns + "-latch").asScala.toList
    assert(l3.size === 1)
    haService.reset()
    Thread.sleep(5000)
    val l4 = zooKeeperClient.getChildren.forPath("/" + ns + "-latch").asScala.toList
    assert(l4.size === 1)
    assert(l3.head !== l4.head)

    zooKeeperClient.delete().forPath("/" + ns + "/" + l2.head)
    Thread.sleep(5000)
    val l5 = zooKeeperClient.getChildren.forPath("/" + ns + "-latch").asScala.toList
    assert(l5.size === 1)
    assert(l5.head !== l4.head)
  }
}
