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
class EmbeddedZkServer private(name: String) extends AbstractService(name) {

  def this() = this(classOf[EmbeddedZkServer].getSimpleName)

  private var server: TestingServer = _

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    val port = conf.get(KyuubiConf.EMBEDDED_ZK_PORT)
    val dataDir = conf.get(KyuubiConf.EMBEDDED_ZK_TEMP_DIR)
    server = new TestingServer(port, new File(dataDir), false)
    super.initialize(conf)
  }

  override def start(): Unit = {
    server.start()
    super.start()
  }

  override def stop(): Unit = {
    if (server != null) {
      server.close()
    }
    server = null
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
