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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.server.EmbeddedZkServer
import org.apache.kyuubi.service.CompositeService

object KyuubiServer {

  def main(args: Array[String]): Unit = {
    val conf = new KyuubiConf().loadFileDefaults()
    val server = new KyuubiServer()
    server.initialize(conf)
    server.start()

    Thread.sleep(10000)
    print("Hello Kyuubi")
  }
}

class KyuubiServer(name: String) extends CompositeService(name) {

  def this() = this(classOf[KyuubiServer].getName)

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    val zkServer = new EmbeddedZkServer()
    addService(zkServer)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
  }

  override def stop(): Unit = {
    super.stop()
  }

}
