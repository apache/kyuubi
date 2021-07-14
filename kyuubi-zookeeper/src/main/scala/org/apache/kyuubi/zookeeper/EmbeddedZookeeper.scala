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

import java.io.File
import java.net.InetAddress

import scala.collection.JavaConverters._

import org.apache.curator.test.{InstanceSpec, TestingServer}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.{AbstractService, ServiceState}
import org.apache.kyuubi.zookeeper.ZookeeperConf._

class EmbeddedZookeeper extends AbstractService("EmbeddedZookeeper") {
  private var spec: InstanceSpec = _
  private var server: TestingServer = _

  // When the client port is 0, the TestingServer will not randomly pick free local port to use
  // So adjust it to -1 to achieve what is common cognition.
  private def normalizePort(port: Int): Int = if (port == 0) -1 else port

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val dataDirectory = new File(conf.get(ZK_DATA_DIR))
    val clientPort = normalizePort(conf.get(ZK_CLIENT_PORT))
    val electionPort = normalizePort(conf.get(ZK_ELECTION_PORT))
    val quorumPort = normalizePort(conf.get(ZK_QUORUM_PORT))
    val serverId = conf.get(ZK_SERVER_ID)
    val tickTime = conf.get(ZK_TICK_TIME)
    val maxClientCnxns = conf.get(ZK_MAX_CLIENT_CONNECTIONS)
    // TODO: Is it right in prod?
    val deleteDataDirectoryOnClose = true
    val customProperties =
      (conf.get(ZK_MIN_SESSION_TIMEOUT).map { timeout =>
        "minSessionTimeout" -> Integer.valueOf(timeout)
      } ++ conf.get(ZK_MAX_SESSION_TIMEOUT).map { timeout =>
        "maxSessionTimeout" -> Integer.valueOf(timeout)
      }).toMap[String, Object].asJava

    val hostname = conf.get(ZK_CLIENT_PORT_ADDRESS).map(InetAddress.getByName)
      .getOrElse(Utils.findLocalInetAddress).getCanonicalHostName
    spec = new InstanceSpec(
      dataDirectory,
      clientPort,
      electionPort,
      quorumPort,
      deleteDataDirectoryOnClose,
      serverId,
      tickTime,
      maxClientCnxns,
      customProperties,
      hostname)
    server = new TestingServer(spec, false)
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    server.start()
    info(s"$getName is started at $getConnectString")
    Utils.addShutdownHook(() => server.close(), 50)
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (getServiceState == ServiceState.STARTED) {
      server.close()
    }
    super.stop()
  }

  def getConnectString: String = synchronized {
    assert(spec != null, s"$getName is in $getServiceState")
    spec.getConnectString
  }
}
