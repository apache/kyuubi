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
import org.apache.kyuubi.engine.jdbc.{WithExternalJdbcEngine, WithJdbcEngine}

trait WithMySQLEngine extends WithJdbcEngine with TestContainerForAll with WithExternalJdbcEngine {

  private val mysqlDockerImage = "mysql:8.0.32"

  override val containerDef: MySQLContainer.Def = MySQLContainer.Def(
    dockerImageName = DockerImageName.parse(mysqlDockerImage),
    username = "root",
    password = "kyuubi")

  // Optional escape hatch: when KYUUBI_TEST_MYSQL_URL is set, talk to a long-running MySQL
  // (e.g. one started by `dev/jdbc-tests/docker-compose.yml`) instead of a per-suite
  // Testcontainers instance. Useful on hosts where Testcontainers is slow or where the
  // image lacks a native build for the local CPU architecture.
  private lazy val externalMySQLUrl: Option[String] = sys.env.get("KYUUBI_TEST_MYSQL_URL")
  override protected def externalJdbcUrl: Option[String] = externalMySQLUrl
  private def externalMySQLUser: String = sys.env.getOrElse("KYUUBI_TEST_MYSQL_USER", "root")
  private def externalMySQLPassword: String =
    sys.env.getOrElse("KYUUBI_TEST_MYSQL_PASSWORD", "kyuubi")

  override def withKyuubiConf: Map[String, String] = externalMySQLUrl match {
    case Some(url) => Map(
        ENGINE_SHARE_LEVEL.key -> "SERVER",
        ENGINE_JDBC_CONNECTION_URL.key -> url,
        ENGINE_JDBC_CONNECTION_USER.key -> externalMySQLUser,
        ENGINE_JDBC_CONNECTION_PASSWORD.key -> externalMySQLPassword,
        ENGINE_TYPE.key -> "jdbc",
        ENGINE_JDBC_SHORT_NAME.key -> "mysql",
        KYUUBI_SESSION_USER_KEY -> "kyuubi",
        ENGINE_JDBC_DRIVER_CLASS.key -> "com.mysql.cj.jdbc.Driver")
    case None => withContainers { mysqlContainer =>
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
}
