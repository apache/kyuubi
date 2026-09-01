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

package org.apache.kyuubi.ha.client

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_FLIGHT_SQL_NAMESPACE, HA_NAMESPACE}
import org.apache.kyuubi.service.FrontendService

/**
 * Service discovery for the Arrow Flight SQL frontend.
 * Registers under [[HA_FLIGHT_SQL_NAMESPACE]] so Flight gRPC endpoints remain
 * separate from Thrift/JDBC discovery trees.
 */
class FlightSqlServiceDiscovery(fe: FrontendService)
  extends KyuubiServiceDiscovery(fe) {

  override def initialize(conf: KyuubiConf): Unit = {
    val discoveryConf = conf.clone
    discoveryConf.set(HA_NAMESPACE, conf.get(HA_FLIGHT_SQL_NAMESPACE))
    super.initialize(discoveryConf)
  }
}
