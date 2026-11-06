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

import java.util.Properties
import javax.security.sasl.AuthenticationException
import javax.sql.DataSource

import com.zaxxer.hikari.util.DriverDataSource
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.util.JdbcUtils

class JdbcAuthenticationProviderImpl(conf: KyuubiConf) extends PasswdAuthenticationProvider
  with Logging {

  private val SQL_PLACEHOLDER_REGEX = """\$\{.+?}""".r
  private val USER_SQL_PLACEHOLDER = "${user}"
  private val PASSWORD_SQL_PLACEHOLDER = "${password}"
  private val supportedPlaceholders = Set(
    USER_SQL_PLACEHOLDER,
    PASSWORD_SQL_PLACEHOLDER)

  private val driverClass = conf.get(AUTHENTICATION_JDBC_DRIVER)
  private val authDbJdbcUrl = conf.get(AUTHENTICATION_JDBC_URL)
  private val authDbUser = conf.get(AUTHENTICATION_JDBC_USER)
  private val authDbPassword = conf.get(AUTHENTICATION_JDBC_PASSWORD)
  private val authQuery = conf.get(AUTHENTICATION_JDBC_QUERY)

  checkJdbcConfigs()

  implicit private[kyuubi] val ds: DataSource = new DriverDataSource(
    authDbJdbcUrl.orNull,
    driverClass.orNull,
    new Properties,
    authDbUser.orNull,
    authDbPassword.orNull)

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

    try {
      debug(s"prepared auth query: $preparedQuery")
      JdbcUtils.executeQuery(preparedQuery) { pStmt =>
        queryPlaceholders.zipWithIndex.foreach {
          case (USER_SQL_PLACEHOLDER, i) => pStmt.setString(i + 1, user)
          case (PASSWORD_SQL_PLACEHOLDER, i) => pStmt.setString(i + 1, password)
          case (p, _) => throw new IllegalArgumentException(
              s"Unrecognized placeholder in Query SQL: $p")
        }
        pStmt.setMaxRows(1) // skipping more result rows to minimize I/O
      } { resultSet =>
        if (!resultSet.next()) {
          throw new AuthenticationException("Password does not match or no such user. " +
            s"user: $user, password: ${JdbcUtils.redactPassword(Some(password))}")
        }
      }
    } catch {
      case rethrow: AuthenticationException =>
        throw rethrow
      case rethrow: Exception =>
        throw new AuthenticationException("Cannot get user info", rethrow)
    }
  }

  private def checkJdbcConfigs(): Unit = {
    val configLog = (config: String, value: Option[String]) =>
      s"JDBCAuthConfig: $config = '${value.orNull}'"

    debug(configLog("Driver Class", driverClass))
    debug(configLog("JDBC URL", authDbJdbcUrl))
    debug(configLog("Database user", authDbUser))
    debug(configLog("Database password", Some(JdbcUtils.redactPassword(authDbPassword))))
    debug(configLog("Query SQL", authQuery))

    // Check if JDBC parameters valid
    require(driverClass.nonEmpty, "JDBC driver class is not configured.")
    require(authDbJdbcUrl.nonEmpty, "JDBC url is not configured.")
    // allow empty auth db user or password
    require(authQuery.nonEmpty, "Query SQL is not configured")

    val query = authQuery.get.trim.toLowerCase
    // allow simple select query sql only, complex query like CTE is not allowed
    require(query.startsWith("select"), "Query SQL must start with 'SELECT'")
    if (!query.contains("where")) {
      warn("Query SQL does not contains 'WHERE' keyword")
    }
    if (!query.contains(USER_SQL_PLACEHOLDER)) {
      warn(s"Query SQL does not contains '$USER_SQL_PLACEHOLDER' placeholder")
    }
    if (!query.contains(PASSWORD_SQL_PLACEHOLDER)) {
      warn(s"Query SQL does not contains '$PASSWORD_SQL_PLACEHOLDER' placeholder")
    }

    queryPlaceholders.foreach { placeholder =>
      require(
        supportedPlaceholders.contains(placeholder),
        s"Unsupported placeholder in Query SQL: $placeholder")
    }
  }

  private def preparedQuery: String =
    SQL_PLACEHOLDER_REGEX.replaceAllIn(authQuery.get, "?")

  private def queryPlaceholders: Iterator[String] =
    SQL_PLACEHOLDER_REGEX.findAllMatchIn(authQuery.get).map(_.matched)
}
