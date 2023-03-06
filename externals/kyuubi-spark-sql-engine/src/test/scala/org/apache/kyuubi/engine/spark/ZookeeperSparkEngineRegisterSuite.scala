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

package org.apache.kyuubi.engine.spark

import java.util.UUID

import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_ID, KYUUBI_ENGINE_URL}
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperClientProvider

class ZookeeperSparkEngineRegisterSuite extends WithDiscoverySparkSQLEngine
  with WithEmbeddedZookeeper {

  override def withKyuubiConf: Map[String, String] =
    super.withKyuubiConf ++ zookeeperConf ++ Map(
      "spark.ui.enabled" -> "true")

  override val namespace: String = s"/kyuubi/deregister_test/${UUID.randomUUID.toString}"

  test("Spark Engine Register Zookeeper with spark ui info") {
    zookeeperConf.foreach(entry => kyuubiConf.set(entry._1, entry._2))
    val client = ZookeeperClientProvider.buildZookeeperClient(kyuubiConf)
    client.start()
    val info = client.getChildren.forPath(namespace).get(0).split(";")
    assert(info.exists(_.startsWith(KYUUBI_ENGINE_ID)))
    assert(info.exists(_.startsWith(KYUUBI_ENGINE_URL)))
  }
}
