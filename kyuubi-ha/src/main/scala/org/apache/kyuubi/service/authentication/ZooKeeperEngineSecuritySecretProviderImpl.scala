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

package org.apache.kyuubi.service.authentication

import java.nio.charset.StandardCharsets

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_ENGINE_SECURE_SECRET_NODE
import org.apache.kyuubi.ha.client.DiscoveryClientProvider

class ZooKeeperEngineSecuritySecretProviderImpl extends EngineSecuritySecretProvider {
  import DiscoveryClientProvider._

  private var conf: KyuubiConf = _

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
  }

  override def getSecret(): String = {
    conf.get(HA_ZK_ENGINE_SECURE_SECRET_NODE).map { zkNode =>
      withDiscoveryClient[String](conf) { discoveryClient =>
        new String(discoveryClient.getData(zkNode), StandardCharsets.UTF_8)
      }
    }.getOrElse(
      throw new IllegalArgumentException(s"${HA_ZK_ENGINE_SECURE_SECRET_NODE.key} is not defined"))
  }
}
