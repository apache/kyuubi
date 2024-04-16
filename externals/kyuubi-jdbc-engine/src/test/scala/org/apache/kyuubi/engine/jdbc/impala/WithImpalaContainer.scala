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
package org.apache.kyuubi.engine.jdbc.impala

import java.io.File

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.testcontainers.containers.wait.strategy.Wait

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithImpalaContainer extends WithJdbcServerContainer {
  private val METASTORE_SERVICE_NAME = "metastore"
  private val METASTORE_PORT = 9083

  private val STATESTORE_SERVICE_NAME = "statestored"
  private val STATESTORE_PORT = 25010

  private val CATALOGD_SERVICE_NAME = "catalogd"
  private val CATALOGD_PORT = 25020

  private val IMPALAD_SERVICE_NAME = "impalad"
  private val IMPALAD_PORT = 21050

  override val containerDef: DockerComposeContainer.Def =
    DockerComposeContainer
      .Def(
        composeFiles = new File(Utils.getContextOrKyuubiClassLoader
          .getResource("impala-compose.yml").toURI),
        exposedServices = Seq[ExposedService](
          ExposedService(
            METASTORE_SERVICE_NAME,
            METASTORE_PORT,
            waitStrategy = Wait.forListeningPort),
          ExposedService(
            STATESTORE_SERVICE_NAME,
            STATESTORE_PORT,
            waitStrategy = Wait.forListeningPort),
          ExposedService(
            CATALOGD_SERVICE_NAME,
            CATALOGD_PORT,
            waitStrategy = Wait.forListeningPort),
          ExposedService(
            IMPALAD_SERVICE_NAME,
            IMPALAD_PORT,
            waitStrategy = Wait.forListeningPort)))

  protected def hiveServerJdbcUrl: String = withContainers { container =>
    val feHost: String = container.getServiceHost(IMPALAD_SERVICE_NAME, IMPALAD_PORT)
    val fePort: Int = container.getServicePort(IMPALAD_SERVICE_NAME, IMPALAD_PORT)
    s"jdbc:kyuubi://$feHost:$fePort/;auth=noSasl"
  }
}
