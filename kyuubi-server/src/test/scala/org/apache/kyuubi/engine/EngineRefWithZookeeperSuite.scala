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
import java.util.concurrent.Executors

import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.EngineType.HIVE_SQL
import org.apache.kyuubi.engine.EngineType.SPARK_SQL
import org.apache.kyuubi.engine.ShareLevel.USER
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.DiscoveryClientProvider
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper
import org.apache.kyuubi.zookeeper.ZookeeperConf

class EngineRefWithZookeeperSuite extends EngineRefTests {

  private val zkServer = new EmbeddedZookeeper
  val conf = KyuubiConf()

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

  def getConnectString(): String = zkServer.getConnectString

  // TODO mvoe to EngineRefTests when etcd discovery support more engines
  // KYUUBI #2827 remove all engines dependencies except to spark from server
  ignore("different engine type should use its own lock") {
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)
    conf.set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(KyuubiConf.ENGINE_INIT_TIMEOUT, 3000L)
    conf.set(HighAvailabilityConf.HA_NAMESPACE, "engine_test1")
    conf.set(HighAvailabilityConf.HA_ADDRESSES, getConnectString())
    val conf1 = conf.clone
    conf1.set(KyuubiConf.ENGINE_TYPE, SPARK_SQL.toString)
    val conf2 = conf.clone
    conf2.set(KyuubiConf.ENGINE_TYPE, HIVE_SQL.toString)

    val start = System.currentTimeMillis()
    val times = new Array[Long](2)
    val executor = Executors.newFixedThreadPool(2)
    try {
      executor.execute(() => {
        DiscoveryClientProvider.withDiscoveryClient(conf1) { client =>
          try {
            new EngineRef(conf1, user, "grp", UUID.randomUUID().toString, null)
              .getOrCreate(client)
          } finally {
            times(0) = System.currentTimeMillis()
          }
        }
      })
      executor.execute(() => {
        DiscoveryClientProvider.withDiscoveryClient(conf2) { client =>
          try {
            new EngineRef(conf2, user, "grp", UUID.randomUUID().toString, null)
              .getOrCreate(client)
          } finally {
            times(1) = System.currentTimeMillis()
          }
        }
      })

      eventually(timeout(10.seconds), interval(200.milliseconds)) {
        assert(times.forall(_ > start))
        // ENGINE_INIT_TIMEOUT is 3000ms
        assert(times.max - times.min < 2500)
      }
    } finally {
      executor.shutdown()
    }
  }
}
