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

import java.io.File
import java.nio.file.Paths

import org.apache.flink.configuration.{Configuration, GlobalConfiguration, RestOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

trait WithFlinkSQLEngineLocal extends KyuubiFunSuite with WithFlinkTestResources {

  protected val flinkConfig = new Configuration()
  protected var miniCluster: MiniCluster = _
  protected var engine: FlinkSQLEngine = _
  // conf will be loaded when flink engine starts
  def withKyuubiConf: Map[String, String]
  protected val kyuubiConf: KyuubiConf = FlinkSQLEngine.kyuubiConf

  protected var connectionUrl: String = _

  override def beforeAll(): Unit = {
    withKyuubiConf.foreach { case (k, v) =>
      if (k.startsWith("flink.")) {
        flinkConfig.setString(k.stripPrefix("flink."), v)
      }
    }
    startMiniCluster()
    startFlinkEngine()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopFlinkEngine()
    miniCluster.close()
  }

  def startFlinkEngine(): Unit = {
    withKyuubiConf.foreach { case (k, v) =>
      System.setProperty(k, v)
      kyuubiConf.set(k, v)
    }
    val flinkConfDir = sys.env.getOrElse(
      "FLINK_CONF_DIR", {
        val flinkHome = sys.env.getOrElse(
          "FLINK_HOME", {
            // detect the FLINK_HOME by flink-core*.jar location if unset
            val jarLoc =
              classOf[GlobalConfiguration].getProtectionDomain.getCodeSource.getLocation
            new File(jarLoc.toURI).getParentFile.getParent
          })
        Paths.get(flinkHome, "conf").toString
      })
    val engineContext = FlinkEngineUtils.getDefaultContext(
      new Array[String](0),
      flinkConfig,
      flinkConfDir)
    FlinkSQLEngine.startEngine(engineContext)
    engine = FlinkSQLEngine.currentEngine.get
    connectionUrl = engine.frontendServices.head.connectionUrl
  }

  def stopFlinkEngine(): Unit = {
    if (engine != null) {
      engine.stop()
      engine = null
    }
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

  protected def getJdbcUrl: String = s"jdbc:hive2://$connectionUrl/;"

}
