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

package org.apache.kyuubi.ha.server

import java.io.File

import org.apache.curator.test.TestingServer

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

/**
 * An embedded zookeeper server for testing and quick try with Kyuubi with external
 * zookeeper cluster.
 *
 * @note Avoid to use this for production purpose
 *
 * @param name the service name
 */
class EmbeddedZkServer private(name: String) extends AbstractService(name) with Logging {

  def this() = this(classOf[EmbeddedZkServer].getSimpleName)

  private var server: TestingServer = _

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    var port = conf.get(KyuubiConf.EMBEDDED_ZK_PORT)
    // When the client port is 0, the TestingServer will not randomly pick free local port to use
    // So adjust it to -1 to achieve what is common cognition.
    if (port == 0) port = -1
    val dataDir = conf.get(KyuubiConf.EMBEDDED_ZK_TEMP_DIR)
    server = new TestingServer(port, new File(dataDir), false)
    super.initialize(conf)
  }

  override def start(): Unit = {
    server.start()
    // Just a tradeoff, otherwise we may get NPE if we call stop() immediately.
    // (e.g. the unit test EmbeddedZkServerSuite)
    // TestingZooKeeperMain has a bug that the CountDownLatch released before cnxnFactory inited.
    // More details could see TestingZooKeeperMain.runFromConfig.
    Thread.sleep(5000)
    info(s"$getName is started at $getConnectString")
    super.start()
  }

  override def stop(): Unit = {
    if (server != null) {
      server.close()
      server = null
    }
    super.stop()
  }

  def getConnectString: String = {
    if (server == null) {
      null
    } else {
      server.getConnectString
    }
  }
}
