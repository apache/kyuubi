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

package org.apache.kyuubi.kubernetes.test

import org.apache.kyuubi.Logging
import org.apache.kyuubi.operation.JDBCTests

class KubernetesJDBCTestsSuite extends JDBCTests with Logging {
  private lazy val _jdbcUrl: String = {
    val kubernetesclient = MiniKube.getKubernetesClient
    val kyuubiServers =
      kubernetesclient
        .pods()
        .list()
        .getItems
    assert(kyuubiServers.size() == 1)
    val kyuubiServer = kyuubiServers.get(0)
    // Kyuubi server state should be running since mvn compile is quite slowly..
    if (!"running".equalsIgnoreCase(kyuubiServer.getStatus.getPhase)) {
      throw new IllegalStateException(
        s"Kyuubi server pod state error: ${kyuubiServer.getStatus.getPhase}")
    }
    val kyuubiServerIp = MiniKube.getIp
    val kyuubiServerPort =
      kyuubiServer.getSpec.getContainers.get(0).getPorts.get(0).getHostPort
    s"jdbc:hive2://$kyuubiServerIp:$kyuubiServerPort/;"
  }

  override protected def jdbcUrl: String = {
    assert(_jdbcUrl != null, "Failed to get Kyuubi server")
    _jdbcUrl
  }
}
