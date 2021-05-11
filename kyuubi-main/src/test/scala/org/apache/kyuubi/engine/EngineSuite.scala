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

package org.apache.kyuubi.engine

import org.apache.curator.utils.ZKPaths
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.util.NamedThreadFactory
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class EngineSuite extends KyuubiFunSuite {
  import ShareLevel._
  private val zkServer = new EmbeddedZookeeper
  private val conf = KyuubiConf()
  val user = Utils.currentUser

  override def beforeAll(): Unit = {
    val zkData = Utils.createTempDir()
    conf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
      .set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    zkServer.initialize(conf)
    zkServer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    zkServer.stop()
    super.afterAll()
  }

  test(s"${CONNECTION} shared level engine name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    Seq(None, Some("suffix")).foreach { domain =>
      conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, CONNECTION.toString)
      domain.foreach(conf.set(KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN.key, _))
      val engine = Engine(conf, user, id)
      assert(engine.engineSpace ===
        ZKPaths.makePath(s"kyuubi_$CONNECTION", user, id.identifier.toString))
      assert(engine.defaultEngineName === s"kyuubi_${CONNECTION}_${user}_${id.identifier}")
    }
  }

  test(s"${USER} shared level engine name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.unset(KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN)
    val appName = Engine(conf, user, id)
    assert(appName.engineSpace === ZKPaths.makePath(s"kyuubi_$USER", user))
    assert(appName.defaultEngineName === s"kyuubi_${USER}_${user}_${id.identifier}")

    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN.key, "abc")
    val appName2 = Engine(conf, user, id)
    assert(appName2.engineSpace ===
      ZKPaths.makePath(s"kyuubi_$USER", user, "abc"))
    assert(appName2.defaultEngineName === s"kyuubi_${USER}_${user}_abc_${id.identifier}")
  }

  test(s"${SERVER} shared level engine name") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, SERVER.toString)
    conf.unset(KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN)
    val appName = Engine(conf, user, id)
    assert(appName.engineSpace ===
      ZKPaths.makePath(s"kyuubi_$SERVER", user))
    assert(appName.defaultEngineName ===  s"kyuubi_${SERVER}_${user}_${id.identifier}")

    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN.key, "abc")
    val appName2 = Engine(conf, user, id)
    assert(appName2.engineSpace ===
      ZKPaths.makePath(s"kyuubi_$SERVER", user, "abc"))
    assert(appName2.defaultEngineName ===  s"kyuubi_${SERVER}_${user}_abc_${id.identifier}")
  }

  test("start and get engine address with lock") {
    val id = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_ZK_NAMESPACE, "engine_test")
    conf.set(HighAvailabilityConf.HA_ZK_QUORUM, zkServer.getConnectString)
    val engine = Engine(conf, user, id)

    var port1 = 0
    var port2 = 0

    val r1 = new Runnable {
      override def run(): Unit = {
        ServiceDiscovery.withZkClient(conf) { client =>
          val hp = engine.start(client)
          port1 = hp._2
        }
      }
    }

    val r2 = new Runnable {
      override def run(): Unit = {
        ServiceDiscovery.withZkClient(conf) { client =>
          val hp = engine.start(client)
          port2 = hp._2
        }
      }
    }
    val factory = new NamedThreadFactory("engine-test", false)
    val thread1 = factory.newThread(r1)
    val thread2 = factory.newThread(r2)
    thread1.start()
    thread2.start()

    eventually(timeout(90.seconds), interval(100.microseconds)) {
      assert(port1 != 0, "engine started")
      assert(port2 == port1, "engine shared")
    }
  }
}
