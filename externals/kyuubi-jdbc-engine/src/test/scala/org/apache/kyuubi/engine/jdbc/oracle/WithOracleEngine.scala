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

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_PASSWORD, ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_CONNECTION_USER, ENGINE_JDBC_DRIVER_CLASS, ENGINE_JDBC_SHORT_NAME, ENGINE_SHARE_LEVEL, ENGINE_TYPE, FRONTEND_BIND_HOST}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.jdbc.WithJdbcEngine

trait WithOracleEngine extends WithJdbcEngine with WithOracleContainer {

  override def withKyuubiConf: Map[String, String] = withContainers { container =>
    Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_JDBC_CONNECTION_URL.key -> container.jdbcUrl,
      ENGINE_JDBC_CONNECTION_USER.key -> container.username,
      ENGINE_JDBC_CONNECTION_PASSWORD.key -> container.password,
      ENGINE_TYPE.key -> "jdbc",
      ENGINE_JDBC_SHORT_NAME.key -> "oracle",
      FRONTEND_BIND_HOST.key -> "0.0.0.0",
      KYUUBI_SESSION_USER_KEY -> "kyuubi",
      ENGINE_JDBC_DRIVER_CLASS.key -> "oracle.jdbc.OracleDriver")
  }

}
