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

// import java.time.Duration

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.{Wait, WaitAllStrategy}

import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithOracleContainer extends WithJdbcServerContainer {
  private val oraclePort = 1521
  private val ports = Seq(oraclePort)
  private val oracleDockerImage = "gvenzl/oracle-free:23.5-slim"
  private val oracleUsernameKey = "APP_USER"
  private val oraclePasswordKey = "APP_USER_PASSWORD"
  protected val oracleUserName = "kyuubi"
  protected val oraclePassword = "oracle"

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def(
    dockerImage = oracleDockerImage,
    exposedPorts = ports,
    waitStrategy = new WaitAllStrategy()
      // .withStartupTimeout(Duration.ofMinutes(1))
      .withStrategy(Wait.forListeningPort())
      .withStrategy(Wait.forLogMessage(
        "Completed: Pluggable database FREEPDB1 opened read write", 1))
       ,
      env = Map(
      oraclePasswordKey -> oraclePassword,
      oracleUsernameKey -> oracleUserName,
      "ORACLE_RANDOM_PASSWORD" -> "true"))

  protected def oracleJdbcUrl: String = withContainers { container =>
    val queryServerHost: String = container.host
    val queryServerPort: Int = container.mappedPort(oraclePort)
    s"jdbc:oracle:thin:@//$queryServerHost:$queryServerPort/FREEPDB1"
  }
}
