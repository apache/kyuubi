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

package org.apache.kyuubi.engine.jdbc.util

import java.sql.Connection

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.jdbc.connection.ConnectionProvider
import org.apache.kyuubi.engine.jdbc.dialect.{JdbcDialect, JdbcDialects}
import org.apache.kyuubi.util.JdbcUtils

object KyuubiJdbcUtils extends Logging {

  def initializeJdbcSession(kyuubiConf: KyuubiConf, initializationSQLs: Seq[String]): Unit = {
    JdbcUtils.withCloseable(ConnectionProvider.create(kyuubiConf)) { connection =>
      initializeJdbcSession(kyuubiConf, connection, initializationSQLs)
    }
  }

  def initializeJdbcSession(
      kyuubiConf: KyuubiConf,
      connection: Connection,
      initializationSQLs: Seq[String]): Unit = {
    if (initializationSQLs == null || initializationSQLs.isEmpty) {
      return
    }
    try {
      val dialect: JdbcDialect = JdbcDialects.get(kyuubiConf)
      JdbcUtils.withCloseable(dialect.createStatement(connection)) { statement =>
        initializationSQLs.foreach { sql =>
          debug(s"Execute initialization sql: $sql")
          statement.execute(sql)
        }
      }
    } catch {
      case e: Exception =>
        error("Failed to execute initialization sql.", e)
        throw KyuubiSQLException(e)
    }
  }
}
