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
package org.apache.kyuubi.engine.jdbc.postgresql

import com.dimafeng.testcontainers.{GenericContainer, SingleContainer}
import org.testcontainers.containers.wait.strategy.Wait

import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithPostgreSqlContainer extends WithJdbcServerContainer {

  private val POSTGRESQL_PORT = 5432

  private val postgreSqlDockerImage = "postgres"

  override val container: SingleContainer[_] = GenericContainer(
    dockerImage = postgreSqlDockerImage,
    exposedPorts = Seq(POSTGRESQL_PORT),
    env = Map[String, String](
      "POSTGRES_PASSWORD" -> "postgres"),
    waitStrategy = Wait.forListeningPort)

  protected def queryUrl: String = {
    val queryServerHost: String = container.host
    val queryServerPort: Int = container.mappedPort(POSTGRESQL_PORT)
    val url = s"$queryServerHost:$queryServerPort"
    url
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.close()
  }

}
