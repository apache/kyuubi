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
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{DiscoveryClientProvider, ServiceDiscovery}
import org.apache.kyuubi.service.FrontendService

/**
 * A service discovery implementation for the REST frontend.
 * It registers the REST service under a subdirectory "rest" of the
 * HA_NAMESPACE (e.g. "/kyuubi/rest") to distinguish from Thrift endpoints.
 *
 * @param fe the REST frontend service to publish for service discovery
 */
class KyuubiRestServiceDiscovery(
    fe: FrontendService) extends ServiceDiscovery("KyuubiRestServiceDiscovery", fe) {

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf

    _namespace = conf.get(HA_NAMESPACE) + "/rest"
    _discoveryClient = DiscoveryClientProvider.createDiscoveryClient(conf)
    discoveryClient.monitorState(this)
    discoveryClient.createClient()

    super.initialize(conf)
  }

  override def stop(): Unit = synchronized {
    if (!isServerLost.get()) {
      discoveryClient.deregisterService()
      discoveryClient.closeClient()
      gracefulShutdownLatch.await()
    } else {
      warn(s"The discovery service ensemble is LOST")
    }
    super.stop()
  }
}
