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

package org.apache.kyuubi.it.flink

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.util.JavaUtils

trait WithKyuubiServerAndFlinkMiniCluster extends WithKyuubiServer {

  val kyuubiHome: String = JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  protected lazy val flinkConfig = new Configuration()
  protected var miniCluster: MiniCluster = _

  override def beforeAll(): Unit = {
    startMiniCluster()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    miniCluster.close()
  }

  private def startMiniCluster(): Unit = {
    val cfg = new MiniClusterConfiguration.Builder()
      .setConfiguration(flinkConfig)
      .setNumSlotsPerTaskManager(1)
      .build
    miniCluster = new MiniCluster(cfg)
    miniCluster.start()
    flinkConfig.setString(RestOptions.ADDRESS, miniCluster.getRestAddress.get().getHost)
    flinkConfig.setInteger(RestOptions.PORT, miniCluster.getRestAddress.get().getPort)
  }
}
