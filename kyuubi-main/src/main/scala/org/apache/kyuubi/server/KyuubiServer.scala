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

package org.apache.kyuubi.server

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.server.EmbeddedZkServer
import org.apache.kyuubi.service.{AbstractBackendService, SeverLike}
import org.apache.kyuubi.util.SignalRegister

object KyuubiServer extends Logging {

  def main(args: Array[String]): Unit = {
    SignalRegister.registerLogger(logger)
    val conf = new KyuubiConf().loadFileDefaults()
    val zkEnsemble = conf.get(HA_ZK_QUORUM)
    if (zkEnsemble == null || zkEnsemble.isEmpty) {
      val zkServer = new EmbeddedZkServer()
      zkServer.initialize(conf)
      zkServer.start()
      conf.set(HA_ZK_QUORUM, zkServer.getConnectString)
    }

    val server = new KyuubiServer()
    server.initialize(conf)
    server.start()
  }
}

class KyuubiServer(name: String) extends SeverLike(name) {

  def this() = this(classOf[KyuubiServer].getSimpleName)

  override protected val backendService: AbstractBackendService = new KyuubiBackendService()

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
  }

  override protected def stopServer(): Unit = {}
}
