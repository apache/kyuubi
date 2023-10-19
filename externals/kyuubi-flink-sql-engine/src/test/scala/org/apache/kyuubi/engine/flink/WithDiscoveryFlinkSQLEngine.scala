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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryClientProvider}

trait WithDiscoveryFlinkSQLEngine {

  protected def namespace: String

  protected def conf: KyuubiConf

  def withDiscoveryClient(f: DiscoveryClient => Unit): Unit = {
    DiscoveryClientProvider.withDiscoveryClient(conf)(f)
  }

  def getFlinkEngineServiceUrl: String = {
    var hostPort: Option[(String, Int)] = None
    var retries = 0
    while (hostPort.isEmpty && retries < 10) {
      withDiscoveryClient(client => hostPort = client.getServerHost(namespace))
      retries += 1
      Thread.sleep(1000L)
    }
    if (hostPort.isEmpty) {
      throw new RuntimeException("Time out retrieving Flink engine service url.")
    }
    // delay the access to thrift service because the thrift service
    // may not be ready although it's registered
    Thread.sleep(3000L)
    s"jdbc:hive2://${hostPort.get._1}:${hostPort.get._2}"
  }
}
