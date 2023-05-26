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

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.jdbc.connection.ConnectionProvider
import org.apache.kyuubi.engine.jdbc.dialect.{JdbcDialect, JdbcDialects}
import org.apache.kyuubi.util.JdbcUtils

object KyuubiJdbcUtils extends Logging {

  def initializeJdbcSession(kyuubiConf: KyuubiConf): Unit = {
    try {
      val dialect: JdbcDialect = JdbcDialects.get(kyuubiConf)
      JdbcUtils.withCloseable(ConnectionProvider.create(kyuubiConf)) { connection =>
        JdbcUtils.withCloseable(dialect.createStatement(connection, 100)) { statement =>
          dialect.initializationSQLs().foreach { sql =>
            debug(s"Execute initialization sql: $sql")
            statement.execute(sql)
          }
        }
      }
    } catch {
      case e: Exception =>
        error("Failed to execute initialization sql.", e)
        throw KyuubiSQLException(e)
    }
  }

}
