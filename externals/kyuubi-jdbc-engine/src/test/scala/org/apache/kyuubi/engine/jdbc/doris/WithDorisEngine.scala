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
package org.apache.kyuubi.engine.jdbc.doris

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.jdbc.{WithExternalJdbcEngine, WithJdbcEngine}

trait WithDorisEngine extends WithJdbcEngine with WithDorisContainer with WithExternalJdbcEngine {

  // Optional escape hatch: see `WithMySQLEngine` for rationale. When KYUUBI_TEST_DORIS_URL
  // is set, point at a long-running Doris FE instead of starting a per-suite Testcontainers
  // compose stack.
  private lazy val externalDorisUrl: Option[String] = sys.env.get("KYUUBI_TEST_DORIS_URL")
  override protected def externalJdbcUrl: Option[String] = externalDorisUrl
  private def externalDorisUser: String = sys.env.getOrElse("KYUUBI_TEST_DORIS_USER", "root")
  private def externalDorisPassword: String = sys.env.getOrElse("KYUUBI_TEST_DORIS_PASSWORD", "")

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_SHARE_LEVEL.key -> "SERVER",
    ENGINE_JDBC_CONNECTION_URL.key -> externalDorisUrl.getOrElse(feJdbcUrl),
    ENGINE_JDBC_CONNECTION_USER.key -> externalDorisUser,
    ENGINE_JDBC_CONNECTION_PASSWORD.key -> externalDorisPassword,
    ENGINE_TYPE.key -> "jdbc",
    ENGINE_JDBC_SHORT_NAME.key -> "doris",
    KYUUBI_SESSION_USER_KEY -> "kyuubi",
    ENGINE_JDBC_DRIVER_CLASS.key -> "com.mysql.cj.jdbc.Driver")
}
