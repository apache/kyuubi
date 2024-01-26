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
package org.apache.kyuubi.engine.jdbc.redis

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import org.apache.kyuubi.engine.jdbc.WithJdbcServerContainer

trait WithRedisContainer extends WithJdbcServerContainer {

  private val REDIS_PORT = 6379
  private val redisDockerImage = "redis"

  override val containerDef: GenericContainer.Def[GenericContainer] = GenericContainer.Def(
    dockerImage = redisDockerImage,
    exposedPorts = Seq(REDIS_PORT),
    waitStrategy = Wait.forListeningPort())

  protected def redisJdbcUrl: String = withContainers { container =>
    val redisServerHost: String = container.host
    val redisServerPort: Int = container.mappedPort(REDIS_PORT)
    val url = s"jdbc:redis://$redisServerHost:$redisServerPort"
    url
  }
}
