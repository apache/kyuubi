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

package org.apache.kyuubi.zookeeper

import java.util.Properties

import org.apache.zookeeper.server.DatadirCleanupManager
import org.apache.zookeeper.server.quorum.{QuorumPeerConfig, QuorumPeerMain}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.zookeeper.ZookeeperConf._

class EmbeddedZookeeper extends AbstractService("EmbeddedZookeeper") {

  override def initialize(conf: KyuubiConf): Unit = {
    val peerConfig = new QuorumPeerConfig()
    val properties = new Properties()
    properties.setProperty("clientPort", conf.get(ZK_CLIENT_PORT).toString)
    conf.get(ZK_CLIENT_PORT_ADDRESS).foreach(properties.setProperty("clientPortAddress", _))
    properties.setProperty("dataDir", conf.get(ZK_DATA_DIR))
    properties.setProperty("dataLogDir", conf.get(ZK_DATA_LOG_DIR))
    properties.setProperty("tickTime", conf.get(ZK_TICK_TIME).toString)
    properties.setProperty("tickTime", conf.get(ZK_TICK_TIME).toString)
    properties.setProperty("maxClientCnxns", conf.get(ZK_MAX_CLIENT_CONNECTIONS).toString)
    conf.get(ZK_MIN_SESSION_TIMEOUT).foreach { timeout =>
      properties.setProperty("minSessionTimeout", timeout.toString)
    }
    conf.get(ZK_MAX_SESSION_TIMEOUT).foreach { timeout =>
      properties.setProperty("maxSessionTimeout", timeout.toString)
    }
    conf.get(ZK_SERVER_LIST).foreach { servers =>
      servers.foreach { server =>
        val kv = server.split("=")
        val key = kv(0)
        val value = kv(1)
        properties.setProperty(key, value)
      }
    }
    peerConfig.parseProperties(properties)
    val purgeMgr = new DatadirCleanupManager(
      peerConfig.getDataDir,
      peerConfig.getDataLogDir,
      peerConfig.getSnapRetainCount,
      peerConfig.getPurgeInterval)
    purgeMgr.start()
    val quorumPeerMain = new QuorumPeerMain()
    quorumPeerMain.runFromConfig(peerConfig)

    super.initialize(conf)
  }
}

object EmbeddedZookeeper {
  def main(args: Array[String]): Unit = {
    val conf = new KyuubiConf()
    val address = conf.get(ZK_CLIENT_PORT_ADDRESS)
    print(address.getOrElse(""))
  }
}
