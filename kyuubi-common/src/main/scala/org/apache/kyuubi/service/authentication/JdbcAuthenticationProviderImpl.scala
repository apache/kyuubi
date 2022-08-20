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
  private val USERNAME_SQL_PLACEHOLDER = "${username}"
  private val PASSWORD_SQL_PLACEHOLDER = "${password}"

  private val driverClass = conf.get(AUTHENTICATION_JDBC_DRIVER)
  private val jdbcUrl = conf.get(AUTHENTICATION_JDBC_URL)
  private val username = conf.get(AUTHENTICATION_JDBC_USERNAME)
  private val password = conf.get(AUTHENTICATION_JDBC_PASSWORD)
  private val authQuery = conf.get(AUTHENTICATION_JDBC_QUERY)

  private val redactedPasswd = password match {
    case Some(s) if !StringUtils.isBlank(s) => s"${"*" * s.length}(length: ${s.length})"
    case None => "(empty)"
  }

  checkJdbcConfigs()

  implicit private[kyuubi] val ds: DataSource = new DriverDataSource(
    jdbcUrl.orNull,
    driverClass.orNull,
    new Properties,
    username.orNull,
    password.orNull)

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
      JdbcUtils.executeQuery(preparedQuery) { stmt =>
        stmt.setMaxRows(1) // minimum result size required for authentication
        queryPlaceholders.zipWithIndex.foreach {
          case (USERNAME_SQL_PLACEHOLDER, i) => stmt.setString(i + 1, user)
          case (PASSWORD_SQL_PLACEHOLDER, i) => stmt.setString(i + 1, password)
          case (p, _) => throw new IllegalArgumentException(
              s"Unrecognized placeholder in Query SQL: $p")
        }
      } { resultSet =>
        if (resultSet == null || !resultSet.next()) {
          throw new AuthenticationException("Password does not match or no such user. " +
            s"user: $user, password: $redactedPasswd")
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
    def configLog(config: String, value: String): String = s"JDBCAuthConfig: $config = '$value'"

    debug(configLog("Driver Class", driverClass.orNull))
    debug(configLog("JDBC URL", jdbcUrl.orNull))
    debug(configLog("Database username", username.orNull))
    debug(configLog("Database password", redactedPasswd))
    debug(configLog("Query SQL", authQuery.orNull))

    // Check if JDBC parameters valid
    require(driverClass.nonEmpty, "JDBC driver class is not configured.")
    require(jdbcUrl.nonEmpty, "JDBC url is not configured.")
    require(username.nonEmpty, "JDBC username is not configured")
    // allow empty password
    require(authQuery.nonEmpty, "Query SQL is not configured")

    val query = authQuery.get.trim.toLowerCase
    // allow simple select query sql only, complex query like CTE is not allowed
    require(query.startsWith("select"), "Query SQL must start with 'SELECT'")
    if (!query.contains("where")) {
      warn("Query SQL does not contains 'WHERE' keyword")
    }
    if (!query.contains(USERNAME_SQL_PLACEHOLDER)) {
      warn(s"Query SQL does not contains '$USERNAME_SQL_PLACEHOLDER' placeholder")
    }
    if (!query.contains(PASSWORD_SQL_PLACEHOLDER)) {
      warn(s"Query SQL does not contains '$PASSWORD_SQL_PLACEHOLDER' placeholder")
    }
  }

  private def preparedQuery: String =
    SQL_PLACEHOLDER_REGEX.replaceAllIn(authQuery.get, "?")

  private def queryPlaceholders: Iterator[String] =
    SQL_PLACEHOLDER_REGEX.findAllMatchIn(authQuery.get).map(_.matched)
}
