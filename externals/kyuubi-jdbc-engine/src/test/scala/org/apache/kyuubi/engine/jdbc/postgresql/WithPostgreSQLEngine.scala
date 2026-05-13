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

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.jdbc.{WithExternalJdbcEngine, WithJdbcEngine}

trait WithPostgreSQLEngine
  extends WithJdbcEngine with WithPostgreSQLContainer with WithExternalJdbcEngine {

  // Optional escape hatch: see `WithMySQLEngine` for rationale. When KYUUBI_TEST_PG_URL is
  // set, point at a long-running PostgreSQL instead of starting a per-suite Testcontainers
  // instance.
  private lazy val externalPgUrl: Option[String] = sys.env.get("KYUUBI_TEST_PG_URL")
  override protected def externalJdbcUrl: Option[String] = externalPgUrl
  private def externalPgUser: String = sys.env.getOrElse("KYUUBI_TEST_PG_USER", "kyuubi")
  private def externalPgPassword: String =
    sys.env.getOrElse("KYUUBI_TEST_PG_PASSWORD", "postgres")

  override def withKyuubiConf: Map[String, String] = externalPgUrl match {
    case Some(url) => Map(
        ENGINE_SHARE_LEVEL.key -> "SERVER",
        ENGINE_JDBC_CONNECTION_URL.key -> url,
        ENGINE_JDBC_CONNECTION_USER.key -> externalPgUser,
        ENGINE_JDBC_CONNECTION_PASSWORD.key -> externalPgPassword,
        ENGINE_TYPE.key -> "jdbc",
        ENGINE_JDBC_SHORT_NAME.key -> "postgresql",
        KYUUBI_SESSION_USER_KEY -> "kyuubi",
        ENGINE_JDBC_DRIVER_CLASS.key -> "org.postgresql.Driver")
    case None => withContainers { container =>
        Map(
          ENGINE_SHARE_LEVEL.key -> "SERVER",
          ENGINE_JDBC_CONNECTION_URL.key -> container.jdbcUrl,
          ENGINE_JDBC_CONNECTION_USER.key -> container.username,
          ENGINE_JDBC_CONNECTION_PASSWORD.key -> container.password,
          ENGINE_TYPE.key -> "jdbc",
          ENGINE_JDBC_SHORT_NAME.key -> "postgresql",
          KYUUBI_SESSION_USER_KEY -> "kyuubi",
          ENGINE_JDBC_DRIVER_CLASS.key -> container.driverClassName)
      }
  }

}
