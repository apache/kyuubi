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
package org.apache.kyuubi.engine.jdbc.clickhouse

import com.dimafeng.testcontainers.{GenericContainer, SingleContainer}
import org.testcontainers.containers.wait.strategy.Wait

import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithClickHouseContainer extends WithJdbcServerContainer {

  private val CLICKHOUSE_PORT = 8123

  private val clickHouseDockerImage = "clickhouse/clickhouse-docker:1.0"

  override val container: SingleContainer[_] = GenericContainer(
    dockerImage = clickHouseDockerImage,
    exposedPorts = Seq(CLICKHOUSE_PORT),
    env = Map[String, String](
      "CLICKHOUSE_PASSWORD" -> "clickhouse"),
    waitStrategy = Wait.forListeningPort)

  protected def queryServerUrl: String = {
    val queryServerHost: String = container.host
    val queryServerPort: Int = container.mappedPort(CLICKHOUSE_PORT)
    val url = s"$queryServerHost:$queryServerPort"
    url
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.close()
  }

}
