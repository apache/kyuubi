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
import java.net.InetSocketAddress
import java.nio.file.Paths

import org.apache.kyuubi.Utils._
import org.apache.kyuubi.config.{ConfigEntry, KyuubiConf}
import org.apache.kyuubi.service.{AbstractService, ServiceState}
import org.apache.kyuubi.shaded.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.apache.kyuubi.util.JavaUtils
import org.apache.kyuubi.zookeeper.ZookeeperConf._

class EmbeddedZookeeper extends AbstractService("EmbeddedZookeeper") {

  private var zks: ZooKeeperServer = _
  private var serverFactory: NIOServerCnxnFactory = _
  private var dataDirectory: File = _
  private var dataLogDirectory: File = _
  // TODO: Is it right in prod?
  private val deleteDataDirectoryOnClose = true
  private var host: String = _

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    dataDirectory = resolvePathIfRelative(conf, ZK_DATA_DIR)
    dataLogDirectory = resolvePathIfRelative(conf, ZK_DATA_LOG_DIR)

    val clientPort = conf.get(ZK_CLIENT_PORT)
    val tickTime = conf.get(ZK_TICK_TIME)
    val maxClientCnxns = conf.get(ZK_MAX_CLIENT_CONNECTIONS)
    val minSessionTimeout = conf.get(ZK_MIN_SESSION_TIMEOUT)
    val maxSessionTimeout = conf.get(ZK_MAX_SESSION_TIMEOUT)
    host = conf.get(ZK_CLIENT_PORT_ADDRESS).getOrElse {
      if (conf.get(ZK_CLIENT_USE_HOSTNAME)) {
        JavaUtils.findLocalInetAddress.getCanonicalHostName
      } else {
        JavaUtils.findLocalInetAddress.getHostAddress
      }
    }

    try {
      zks = new ZooKeeperServer(dataDirectory, dataLogDirectory, tickTime)
      zks.setMinSessionTimeout(minSessionTimeout)
      zks.setMaxSessionTimeout(maxSessionTimeout)

      serverFactory = new NIOServerCnxnFactory
      serverFactory.configure(new InetSocketAddress(host, clientPort), maxClientCnxns)

      super.initialize(conf)
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Failed to initialize the embedded ZooKeeper server, binding to $host:$clientPort",
          e)
    }
  }

  override def start(): Unit = synchronized {
    serverFactory.startup(zks)
    info(s"$getName is started at $getConnectString")
    // Stop the EmbeddedZookeeper after the Kyuubi server stopped
    addShutdownHook(() => stop(), SERVER_SHUTDOWN_PRIORITY - 1)
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (getServiceState == ServiceState.STARTED) {
      if (null != serverFactory) serverFactory.shutdown()
      if (null != zks) zks.shutdown()
      if (deleteDataDirectoryOnClose) {
        deleteDirectoryRecursively(dataDirectory)
        deleteDirectoryRecursively(dataLogDirectory)
      }
    }
    super.stop()
  }

  def getConnectString: String = synchronized {
    assert(zks != null, s"$getName is in $getServiceState")
    s"$host:${serverFactory.getLocalPort}"
  }

  def resolvePathIfRelative(conf: KyuubiConf, configEntry: ConfigEntry[String]): File = {
    val dirFromConfig = conf.get(configEntry)
    val KYUUBI_HOME = sys.env.getOrElse(KyuubiConf.KYUUBI_HOME_ENV_VAR_NAME, ".")
    Paths.get(KYUUBI_HOME).resolve(dirFromConfig).toFile
  }

}
