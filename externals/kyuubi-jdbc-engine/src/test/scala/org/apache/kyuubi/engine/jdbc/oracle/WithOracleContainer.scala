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

package org.apache.kyuubi.engine.jdbc.oracle

import java.io.File
import java.time.Duration

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithOracleContainer extends WithJdbcServerContainer {
  private val ORACLE_PORT = 1521
  private val ORACLE_SERVICE_NAME = "oracle"
  protected val ORACLE_USER_NAME = "kyuubi"
  protected val ORACLE_PASSWORD = "oracle"

  override val containerDef: DockerComposeContainer.Def =
    DockerComposeContainer
      .Def(
        composeFiles = new File(Utils.getContextOrKyuubiClassLoader
          .getResource("oracle-compose.yml").toURI),
        exposedServices = Seq[ExposedService](
          ExposedService(
            ORACLE_SERVICE_NAME,
            ORACLE_PORT,
            waitStrategy =
              new DockerHealthcheckWaitStrategy().withStartupTimeout(Duration.ofMinutes(2)))))

  protected def oracleJdbcUrl: String = withContainers { container =>
    val feHost: String = container.getServiceHost(ORACLE_SERVICE_NAME, ORACLE_PORT)
    val fePort: Int = container.getServicePort(ORACLE_SERVICE_NAME, ORACLE_PORT)
    s"jdbc:oracle:thin:@//$feHost:$fePort/FREEPDB1"
  }
}
