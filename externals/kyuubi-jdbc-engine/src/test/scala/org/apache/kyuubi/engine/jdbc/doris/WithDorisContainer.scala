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
package org.apache.kyuubi.engine.jdbc.doris

import java.io.File
import java.time.Duration

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithDorisContainer extends WithJdbcServerContainer {

  private val DORIS_FE_MYSQL_PORT = 9030
  private val DORIS_BE_HTTTP_PORT = 8040

  private val DORIS_FE_SERVICE_NAME = "doris-fe"
  private val DORIS_BE_SERVICE_NAME = "doris-be"

  override val containerDef: DockerComposeContainer.Def =
    DockerComposeContainer
      .Def(
        composeFiles = new File(Utils.getContextOrKyuubiClassLoader
          .getResource("doris-compose.yml").toURI),
        exposedServices = Seq[ExposedService](
          ExposedService(
            DORIS_FE_SERVICE_NAME,
            DORIS_FE_MYSQL_PORT,
            waitStrategy =
              new DockerHealthcheckWaitStrategy().withStartupTimeout(Duration.ofMinutes(5))),
          ExposedService(
            DORIS_BE_SERVICE_NAME,
            DORIS_BE_HTTTP_PORT,
            waitStrategy =
              new DockerHealthcheckWaitStrategy().withStartupTimeout(Duration.ofMinutes(5)))))

  protected def feJdbcUrl: String = withContainers { container =>
    val feHost: String = container.getServiceHost(DORIS_FE_SERVICE_NAME, DORIS_FE_MYSQL_PORT)
    val fePort: Int = container.getServicePort(DORIS_FE_SERVICE_NAME, DORIS_FE_MYSQL_PORT)
    s"jdbc:mysql://$feHost:$fePort"
  }
}
