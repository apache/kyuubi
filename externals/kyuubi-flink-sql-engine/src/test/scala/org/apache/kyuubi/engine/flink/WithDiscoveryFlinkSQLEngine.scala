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

package org.apache.kyuubi.engine.flink

import java.util.UUID

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryClientProvider, DiscoveryPaths}

trait WithDiscoveryFlinkSQLEngine extends WithFlinkSQLEngineOnYarn {

  override protected def engineRefId: String = UUID.randomUUID().toString

  def rootNamespace: String = "/kyuubi/flink-yarn-application-test"

  def shareLevel: String = ShareLevel.USER.toString

  def engineType: String = "flink"

  override def withKyuubiConf: Map[String, String] = {
    Map(HA_NAMESPACE.key -> rootNamespace, ENGINE_SHARE_LEVEL.key -> shareLevel)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }

  def withDiscoveryClient(f: DiscoveryClient => Unit): Unit = {
    DiscoveryClientProvider.withDiscoveryClient(conf)(f)
  }

  def getFlinkEngineServiceUrl: String = {
    var hostPort: (String, Int) = ("0.0.0.0", 0)
    withDiscoveryClient(client => hostPort = client.getServerHost(engineSpace).get)
    s"jdbc:hive2://${hostPort._1}:${hostPort._2}?" +
      s"kyuubi.engine.type=FLINK_SQL;flink.execution.target=yarn-application"
  }

  def engineSpace: String = {
    val commonParent = s"${rootNamespace}_${KYUUBI_VERSION}_${shareLevel}_$engineType"
    val currentUser = UserGroupInformation.getCurrentUser.getShortUserName
    DiscoveryPaths.makePath(commonParent, currentUser)
  }
}
