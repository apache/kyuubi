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

import org.apache.kyuubi.{KYUUBI_VERSION, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.EngineType._
import org.apache.kyuubi.engine.ShareLevel._
import org.apache.kyuubi.ha.client.DiscoveryPaths
import org.apache.kyuubi.plugin.PluginLoader

class EngineRefWithUserPoolSizeSuite extends WithKyuubiServer {
  private val user: String = Utils.currentUser

  protected val poolSelectPolicy = "POLLING"
  protected val defaultEnginePoolSize = 2
  protected val userEnginePoolSize = 3

  override protected val conf: KyuubiConf = {
    val kyuubiConf = KyuubiConf()
      .set(ENGINE_POOL_SELECT_POLICY, poolSelectPolicy)
      .set(ENGINE_POOL_SIZE, defaultEnginePoolSize)
      .set(s"___${user}___.${ENGINE_POOL_SIZE.key}", userEnginePoolSize.toString)
      .set(ENGINE_TYPE, SPARK_SQL.toString)
    kyuubiConf
  }

  test("USER share level engine pool size") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, USER.toString)

    // user customed pool size
    (0 until userEnginePoolSize * 2).foreach { i =>
      val poolEngineSeq = i % userEnginePoolSize
      val appName = new EngineRef(conf, user, PluginLoader.loadGroupProvider(conf), id, null)
      withClue(s"index $i, expected poolEngineSeqde $poolEngineSeq") {
        assertResult(DiscoveryPaths.makePath(
          s"kyuubi_${KYUUBI_VERSION}_${USER}_$SPARK_SQL",
          user,
          s"engine-pool-$poolEngineSeq"))(appName.engineSpace)
        assertResult(s"kyuubi_${USER}_${SPARK_SQL}_${user}_engine-pool-${poolEngineSeq}_$id")(
          appName.defaultEngineName)
      }
    }

    // other users' pool size falling back to default
    val otherUser = "otherUser"
    (0 until userEnginePoolSize * 2).foreach { i =>
      val poolEngineSeq = i % defaultEnginePoolSize
      val appName = new EngineRef(conf, otherUser, PluginLoader.loadGroupProvider(conf), id, null)
      assertResult(DiscoveryPaths.makePath(
        s"kyuubi_${KYUUBI_VERSION}_${USER}_$SPARK_SQL",
        otherUser,
        s"engine-pool-$poolEngineSeq"))(appName.engineSpace)
      assertResult(s"kyuubi_${USER}_${SPARK_SQL}_${otherUser}_engine-pool-${poolEngineSeq}_$id")(
        appName.defaultEngineName)
    }
  }

  test("SERVER share level engine pool size") {
    val id = UUID.randomUUID().toString
    conf.set(KyuubiConf.ENGINE_SHARE_LEVEL, SERVER.toString)

    (0 until userEnginePoolSize * 2).foreach { i =>
      val poolEngineSeq = i % defaultEnginePoolSize
      val appName = new EngineRef(conf, user, PluginLoader.loadGroupProvider(conf), id, null)
      assertResult(DiscoveryPaths.makePath(
        s"kyuubi_${KYUUBI_VERSION}_${SERVER}_$SPARK_SQL",
        user,
        s"engine-pool-$poolEngineSeq"))(appName.engineSpace)
      assertResult(s"kyuubi_${SERVER}_${SPARK_SQL}_${user}_engine-pool-${poolEngineSeq}_$id")(
        appName.defaultEngineName)
    }
  }
}
