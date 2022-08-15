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

package org.apache.kyuubi.service.authentication

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}
import javax.security.sasl.AuthenticationException

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class JdbcAuthenticationProviderImpl(conf: KyuubiConf) extends PasswdAuthenticationProvider
  with Logging {

  private val dbDriver = conf.get(AUTHENTICATION_JDBC_DRIVER)
  private val dbUrl = conf.get(AUTHENTICATION_JDBC_URL)
  private val dbUserName = conf.get(AUTHENTICATION_JDBC_USERNAME)
  private val dbPassword = conf.get(AUTHENTICATION_JDBC_PASSWORD)
  private val querySql = conf.get(AUTHENTICATION_JDBC_QUERY)

  private val SQL_PLACEHOLDER_REGEX = """\$\{.+?}""".r
  private val USERNAME_SQL_PLACEHOLDER = "${username}"
  private val PASSWORD_SQL_PLACEHOLDER = "${password}"

  /**
   * The authenticate method is called by the Kyuubi Server authentication layer
   * to authenticate users for their requests.
   * If a user is to be granted, return nothing/throw nothing.
   * When a user is to be disallowed, throw an appropriate [[AuthenticationException]].
   *
   * @param user     The username received over the connection request
   * @param password The password received over the connection request
   * @throws AuthenticationException When a user is found to be invalid by the implementation
   */
  @throws[AuthenticationException]
  override def authenticate(user: String, password: String): Unit = {
    if (StringUtils.isBlank(user)) {
      throw new AuthenticationException(s"Error validating, user is null" +
        s" or contains blank space")
    }

    if (StringUtils.isBlank(password)) {
      throw new AuthenticationException(s"Error validating, password is null" +
        s" or contains blank space")
    }

    checkConfigs

    // Load Driver Class
    try {
      Class.forName(dbDriver.get)
    } catch {
      case e: ClassNotFoundException =>
        error(s"Driver class not found: $dbDriver")
        throw e;
    }

    var connection: Connection = null
    var queryStatement: PreparedStatement = null

    try {
      connection = DriverManager.getConnection(dbUrl.get, dbUserName.orNull, dbPassword.orNull)

      queryStatement = getAndPrepareStatement(connection, user, password)

      val resultSet = queryStatement.executeQuery()

      if (resultSet == null || !resultSet.next()) {
        // Auth failed
        throw new AuthenticationException(s"Password does not match or no such user. user:" +
          s" $user , password length: ${password.length}")
      }

      // Auth passed

    } catch {
      case e: AuthenticationException =>
        throw e
      case e: Exception =>
        error("Cannot get user info", e);
        throw e
    } finally {
      closeDbConnection(connection, queryStatement)
    }
  }

  private def checkConfigs: Unit = {
    def configLog(config: String, value: String): String = s"JDBCAuthConfig: $config = '$value'"

    debug(configLog("Driver Class", dbDriver.orNull))
    debug(configLog("JDBC URL", dbUrl.orNull))
    debug(configLog("Database Username", dbUserName.orNull))
    debug(configLog("Database Password", dbPassword.orNull))
    debug(configLog("Query SQL", querySql.orNull))

    // Check if JDBC parameters valid
    if (dbDriver.isEmpty || dbUrl.isEmpty || dbUserName.isEmpty || dbPassword.isEmpty) {
      error("User auth Database has not been configured!")
      throw new IllegalArgumentException("User auth Database has not been configured!")
    }

    // Check Query SQL
    if (querySql.isEmpty) {
      error("Query SQL not configured!")
      throw new IllegalArgumentException("Query SQL not configured!")
    }
    if (!querySql.get.trim.toLowerCase.startsWith("select")) { // only allow select query sql
      error("Query SQL must start with \"select\"!")
      throw new IllegalArgumentException("Query SQL must start with \"select\"!");
    }
  }

  /**
   * Extract all placeholders from query and put them into a list.
   *
   * @param sql
   * @return
   */
  private def getPlaceholderList(sql: String): List[String] = {
    SQL_PLACEHOLDER_REGEX.findAllMatchIn(sql)
      .map(m => m.matched)
      .toList
  }

  /**
   * Replace all placeholders as "?"
   *
   * @param sql
   * @return
   */
  private def getPreparedSql(sql: String): String = {
    SQL_PLACEHOLDER_REGEX.replaceAllIn(sql, "?")
  }

  /**
   * prepare the final query statement
   * by replacing placeholder in query sql with user and password
   *
   * @param connection
   * @param user
   * @param password
   * @return
   */
  private def getAndPrepareStatement(
      connection: Connection,
      user: String,
      password: String): PreparedStatement = {
    // Replace placeholders by "?" and prepare the statement
    val stmt = connection.prepareStatement(getPreparedSql(querySql.get))

    // Extract placeholder list and use its order to pass parameters
    val placeholderList: List[String] = getPlaceholderList(querySql.get)
    for (i <- placeholderList.indices) {
      val param = placeholderList(i) match {
        case USERNAME_SQL_PLACEHOLDER => user
        case PASSWORD_SQL_PLACEHOLDER => password
        case otherPlaceholder =>
          error(s"Unrecognized Placeholder In Query SQL: $otherPlaceholder")
          throw new IllegalStateException(
            s"Unrecognized Placeholder In Query SQL: $otherPlaceholder")
      }

      stmt.setString(i + 1, param)
    }

    // Client side limit 1
    stmt.setMaxRows(1)

    stmt
  }

  /**
   * Gracefully close DB connection
   *
   * @param connection
   * @param statement
   */
  private def closeDbConnection(connection: Connection, statement: Statement): Unit = {
    // Close statement
    if (statement != null && !statement.isClosed) {
      try {
        statement.close()
      } catch {
        case e: Exception =>
          error("Cannot close PreparedStatement to auth database ", e)
      }
    }

    // Close connection
    if (connection != null && !connection.isClosed) {
      try {
        connection.close()
      } catch {
        case e: Exception =>
          error("Cannot close connection to auth database ", e)
      }
    }
  }
}
