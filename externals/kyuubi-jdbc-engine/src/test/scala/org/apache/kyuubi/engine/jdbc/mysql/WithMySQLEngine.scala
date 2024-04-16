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
package org.apache.kyuubi.engine.jdbc.mysql

import com.dimafeng.testcontainers.MySQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.testcontainers.utility.DockerImageName

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.jdbc.WithJdbcEngine

trait WithMySQLEngine extends WithJdbcEngine with TestContainerForAll {

  private val mysqlDockerImage = "mysql:8.0.32"

  override val containerDef: MySQLContainer.Def = MySQLContainer.Def(
    dockerImageName = DockerImageName.parse(mysqlDockerImage),
    username = "root",
    password = "kyuubi")

  override def withKyuubiConf: Map[String, String] = withContainers { mysqlContainer =>
    Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_JDBC_CONNECTION_URL.key -> mysqlContainer.jdbcUrl,
      ENGINE_JDBC_CONNECTION_USER.key -> mysqlContainer.username,
      ENGINE_JDBC_CONNECTION_PASSWORD.key -> mysqlContainer.password,
      ENGINE_TYPE.key -> "jdbc",
      ENGINE_JDBC_SHORT_NAME.key -> "mysql",
      KYUUBI_SESSION_USER_KEY -> "kyuubi",
      ENGINE_JDBC_DRIVER_CLASS.key -> "com.mysql.cj.jdbc.Driver")
  }
}
