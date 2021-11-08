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

import java.util.UUID

import org.apache.curator.utils.ZKPaths
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider
import org.apache.kyuubi.util.NamedThreadFactory
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class EngineRefSuite extends KyuubiFunSuite {
  import ShareLevel._
  import EngineType._
  private val zkServer = new EmbeddedZookeeper
  private val conf = KyuubiConf()
  private val user = Utils.currentUser

  override def beforeAll(): Unit = {
    val zkData = Utils.createTempDir()
    conf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
      .set(ZookeeperConf.ZK_CLIENT_PORT, 0)
      .set("spark.sql.catalogImplementation", "in-memory")
    zkServer.initialize(conf)
    zkServer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    zkServer.stop()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    conf.unset(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN)
    conf.unset(KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN)
    super.beforeEach()
  }

  test("CONNECTION shared level engine name") {
    val id = UUID.randomUUID().toString
    val engineType = conf.get(KyuubiConf.ENGINE_TYPE)
    Seq(None, Some("suffix")).foreach { domain =>
      conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, CONNECTION.toString)
      domain.foreach(conf.set(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key, _))
      val engine = new EngineRef(conf, user, id)
      assert(engine.engineSpace ===
        ZKPaths.makePath(s"kyuubi_${CONNECTION}_${engineType}", user, id))
      assert(engine.defaultEngineName === s"kyuubi_${CONNECTION}_${engineType}_${user}_$id")
    }
  }

  test("USER shared level engine name") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, FLINK_SQL.toString)
    val appName = new EngineRef(conf, user, id)
    assert(appName.engineSpace === ZKPaths.makePath(s"kyuubi_${USER}_$FLINK_SQL", user))
    assert(appName.defaultEngineName === s"kyuubi_${USER}_${FLINK_SQL}_${user}_$id")

    Seq(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN,
      KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN).foreach { k =>
      conf.unset(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN)
      conf.set(k.key, "abc")
      val appName2 = new EngineRef(conf, user, id)
      assert(appName2.engineSpace ===
        ZKPaths.makePath(s"kyuubi_${USER}_${FLINK_SQL}", user, "abc"))
      assert(appName2.defaultEngineName === s"kyuubi_${USER}_${FLINK_SQL}_${user}_abc_$id")
    }
  }

  test("GROUP shared level engine name") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, GROUP.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    val engineRef = new EngineRef(conf, user, id)
    val primaryGroupName = UserGroupInformation.createRemoteUser(user).getPrimaryGroupName
    assert(engineRef.engineSpace === ZKPaths.makePath(s"kyuubi_GROUP_SPARK_SQL", primaryGroupName))
    assert(engineRef.defaultEngineName === s"kyuubi_GROUP_SPARK_SQL_${primaryGroupName}_$id")

    Seq(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN,
      KyuubiConf.ENGINE_SHARE_LEVEL_SUB_DOMAIN).foreach { k =>
      conf.unset(k)
      conf.set(k.key, "abc")
      val engineRef2 = new EngineRef(conf, user, id)
      assert(engineRef2.engineSpace ===
        ZKPaths.makePath(s"kyuubi_${GROUP}_${SPARK_SQL}", primaryGroupName, "abc"))
      assert(engineRef2.defaultEngineName ===
        s"kyuubi_${GROUP}_${SPARK_SQL}_${primaryGroupName}_abc_$id")
    }

    val userName = "Iamauserwithoutgroup"
    val newUGI = UserGroupInformation.createRemoteUser(userName)
    assert(newUGI.getGroupNames.isEmpty)
    val engineRef3 = new EngineRef(conf, userName, id)
    assert(engineRef3.engineSpace === ZKPaths.makePath(s"kyuubi_GROUP_SPARK_SQL", userName, "abc"))
    assert(engineRef3.defaultEngineName === s"kyuubi_GROUP_SPARK_SQL_${userName}_abc_$id")
  }

  test("SERVER shared level engine name") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, SERVER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, FLINK_SQL.toString)
    val appName = new EngineRef(conf, user, id)
    assert(appName.engineSpace ===
      ZKPaths.makePath(s"kyuubi_${SERVER}_${FLINK_SQL}", user))
    assert(appName.defaultEngineName ===  s"kyuubi_${SERVER}_${FLINK_SQL}_${user}_$id")

    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "abc")
    val appName2 = new EngineRef(conf, user, id)
    assert(appName2.engineSpace ===
      ZKPaths.makePath(s"kyuubi_${SERVER}_${FLINK_SQL}", user, "abc"))
    assert(appName2.defaultEngineName ===  s"kyuubi_${SERVER}_${FLINK_SQL}_${user}_abc_$id")
  }

  test("check the engine space of engine pool") {
    val id = UUID.randomUUID().toString

    // test subdomain
    conf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "abc")
    val engine1 = new EngineRef(conf, user, id)
    assert(engine1.subdomain === Some("abc"))

    // unset domain
    conf.unset(ENGINE_SHARE_LEVEL_SUBDOMAIN)
    val engine2 = new EngineRef(conf, user, id)
    assert(engine2.subdomain === None)

    // 1 <= engine pool size < threshold
    conf.unset(ENGINE_SHARE_LEVEL_SUBDOMAIN)
    conf.set(ENGINE_POOL_SIZE, 3)
    val engine3 = new EngineRef(conf, user, id)
    assert(engine3.subdomain.get.startsWith("engine-pool-"))

    // engine pool size > threshold
    conf.unset(ENGINE_SHARE_LEVEL_SUBDOMAIN)
    conf.set(ENGINE_POOL_SIZE, 100)
    val engine4 = new EngineRef(conf, user, id)
    val engineNumber = Integer.parseInt(engine4.subdomain.get.substring(12))
    val threshold = ENGINE_POOL_SIZE_THRESHOLD.defaultVal.get
    assert(engineNumber <= threshold)
  }

  test("start and get engine address with lock") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(HighAvailabilityConf.HA_ZK_NAMESPACE, "engine_test")
    conf.set(HighAvailabilityConf.HA_ZK_QUORUM, zkServer.getConnectString)
    val engine = new EngineRef(conf, user, id)

    var port1 = 0
    var port2 = 0

    val r1 = new Runnable {
      override def run(): Unit = {
        ZooKeeperClientProvider.withZkClient(conf) { client =>
          val hp = engine.getOrCreate(client)
          port1 = hp._2
        }
      }
    }

    val r2 = new Runnable {
      override def run(): Unit = {
        ZooKeeperClientProvider.withZkClient(conf) { client =>
          val hp = engine.getOrCreate(client)
          port2 = hp._2
        }
      }
    }
    val factory = new NamedThreadFactory("engine-test", false)
    val thread1 = factory.newThread(r1)
    val thread2 = factory.newThread(r2)
    thread1.start()
    thread2.start()

    eventually(timeout(90.seconds), interval(1.second)) {
      assert(port1 != 0, "engine started")
      assert(port2 == port1, "engine shared")
    }
  }
}
