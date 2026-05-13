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
package org.apache.kyuubi.engine.jdbc.phoenix

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.jdbc.{WithExternalJdbcEngine, WithJdbcEngine}

trait WithPhoenixEngine extends WithJdbcEngine with WithPhoenixContainer
  with WithExternalJdbcEngine {

  private val PHOENIX_DOCKER_IMAGE = "iteblog/hbase-phoenix-docker:1.0"
  private val serialization = "serialization=PROTOBUF"
  private val jdbcUrlPrefix = "jdbc:phoenix:thin:url"

  // Optional escape hatch: see `WithMySQLEngine` for rationale. When
  // KYUUBI_TEST_PHOENIX_URL is set, point at a long-running Phoenix Query Server instead
  // of starting a per-suite Testcontainers instance. Provide the full Phoenix Thin JDBC
  // URL (e.g. "jdbc:phoenix:thin:url=http://host:8765;serialization=PROTOBUF").
  private lazy val externalPhoenixUrl: Option[String] = sys.env.get("KYUUBI_TEST_PHOENIX_URL")
  override protected def externalJdbcUrl: Option[String] = externalPhoenixUrl
  private def externalPhoenixUser: String = sys.env.getOrElse("KYUUBI_TEST_PHOENIX_USER", "root")
  private def externalPhoenixPassword: String =
    sys.env.getOrElse("KYUUBI_TEST_PHOENIX_PASSWORD", "")

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_SHARE_LEVEL.key -> "SERVER",
    ENGINE_JDBC_CONNECTION_URL.key -> externalPhoenixUrl.getOrElse(getConnectString),
    ENGINE_JDBC_CONNECTION_USER.key -> externalPhoenixUser,
    ENGINE_JDBC_CONNECTION_PASSWORD.key -> externalPhoenixPassword,
    ENGINE_TYPE.key -> "jdbc",
    ENGINE_JDBC_SHORT_NAME.key -> "phoenix",
    KYUUBI_SESSION_USER_KEY -> "kyuubi",
    ENGINE_JDBC_DRIVER_CLASS.key -> "org.apache.phoenix.queryserver.client.Driver")

  private def getConnectString: String = s"$jdbcUrlPrefix=http://$queryServerUrl;$serialization"

}
