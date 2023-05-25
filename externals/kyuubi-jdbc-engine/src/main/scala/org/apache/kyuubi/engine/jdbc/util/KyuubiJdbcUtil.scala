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

import java.sql.{Connection, Statement}
import scala.util.{Failure, Success, Try}
import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.jdbc.connection.ConnectionProvider
import org.apache.kyuubi.engine.jdbc.dialect.{JdbcDialect, JdbcDialects}

object KyuubiJdbcUtil extends Logging {

    def initializeJdbcSession(kyuubiConf: KyuubiConf): Unit = {
      var connection: Connection = null
      var statement: Statement = null
      try {
        val dialect: JdbcDialect = JdbcDialects.get(kyuubiConf)
        connection = ConnectionProvider.create(kyuubiConf)
        statement = dialect.createStatement(connection, 100)
        dialect.initializationSQLs().foreach(statement.execute)
      } catch {
        case e: Exception =>
          error("Failed to execute init sql.", e)
          throw KyuubiSQLException(e)
      } finally {
        Try {
          if (statement != null) {
            statement.close()
          }
        } match {
          case Success(_) =>
            info(s"Closed init sql session statement.")
          case Failure(exception) =>
            warn("Failed to close init sql session statement, ignored it.", exception)
        }
        Try {
          if (connection != null) {
            connection.close()
          }
        } match {
          case Success(_) =>
            info(s"Closed init sql session connection.")
          case Failure(exception) =>
            warn("Failed to close init sql session connection, ignored it.", exception)
        }
      }
    }

}