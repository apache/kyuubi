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
package org.apache.kyuubi.engine.jdbc.starrocks

import java.time.Duration

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.{Wait, WaitAllStrategy}
import org.testcontainers.containers.wait.strategy.Wait._

import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithStarRocksContainer extends WithJdbcServerContainer {

  private val starrocksDockerImage = "starrocks/allin1-ubuntu:3.3.13"

  private val STARROCKS_FE_MYSQL_PORT = 9030
  private val STARROCKS_FE_HTTP_PORT = 8030
  private val STARROCKS_BE_THRIFT_PORT = 9060
  private val STARROCKS_BE_HTTP_PORT = 8040
  private val STARROCKS_BE_HEARTBEAT_PORT = 9050
  private val ports = Seq(
    STARROCKS_FE_MYSQL_PORT,
    STARROCKS_FE_HTTP_PORT,
    STARROCKS_BE_THRIFT_PORT,
    STARROCKS_BE_HTTP_PORT,
    STARROCKS_BE_HEARTBEAT_PORT)

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def(
    dockerImage = starrocksDockerImage,
    exposedPorts = ports,
    waitStrategy = new WaitAllStrategy().withStartupTimeout(Duration.ofMinutes(10))
      .withStrategy(Wait.forListeningPorts(ports: _*))
      .withStrategy(forLogMessage(".*broker service already added into FE service.*", 1))
      .withStrategy(
        forLogMessage(".*Enjoy the journey to StarRocks blazing-fast lake-house engine.*", 1)))

  protected def feJdbcUrl: String = withContainers { container =>
    val queryServerHost: String = container.host
    val queryServerPort: Int = container.mappedPort(STARROCKS_FE_MYSQL_PORT)
    s"jdbc:mysql://$queryServerHost:$queryServerPort"
  }
}
