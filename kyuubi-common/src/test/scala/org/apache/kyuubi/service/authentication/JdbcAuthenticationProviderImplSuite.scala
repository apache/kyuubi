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

import java.sql.DriverManager
import java.util.Properties
import javax.security.sasl.AuthenticationException
import javax.sql.DataSource

import com.zaxxer.hikari.util.DriverDataSource

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.util.JdbcUtils

class JdbcAuthenticationProviderImplSuite extends KyuubiFunSuite {
  protected val dbUser: String = "liangbowen"
  protected val dbPasswd: String = "liangbowen"
  protected val authDbName: String = "auth_db"
  protected val dbUrl: String = s"jdbc:derby:memory:$authDbName"
  protected val jdbcUrl: String = s"$dbUrl;create=true"

  implicit private val ds: DataSource = new DriverDataSource(
    jdbcUrl,
    "org.apache.derby.jdbc.AutoloadedDriver",
    new Properties,
    dbUser,
    dbPasswd)

  protected val authUser: String = "liangtiancheng"
  protected val authPasswd: String = "liangtiancheng"

  protected val conf: KyuubiConf = new KyuubiConf()
    .set(AUTHENTICATION_JDBC_DRIVER, "org.apache.derby.jdbc.AutoloadedDriver")
    .set(AUTHENTICATION_JDBC_URL, jdbcUrl)
    .set(AUTHENTICATION_JDBC_USERNAME, dbUser)
    .set(AUTHENTICATION_JDBC_PASSWORD, dbPasswd)
    .set(
      AUTHENTICATION_JDBC_QUERY,
      "SELECT 1 FROM user_auth WHERE username=${username} and passwd=${password}")

  override def beforeAll(): Unit = {
    // init db
    JdbcUtils.execute(s"CREATE SCHEMA $dbUser")()
    JdbcUtils.execute(
      """CREATE TABLE user_auth (
        |  username VARCHAR(64) NOT NULL PRIMARY KEY,
        |  passwd   VARCHAR(64)
        |)""".stripMargin)()
    JdbcUtils.execute("INSERT INTO user_auth (username, passwd) VALUES (?, ?)") { stmt =>
      stmt.setString(1, authUser)
      stmt.setString(2, authPasswd)
    }

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    // cleanup db
    Utils.tryLogNonFatalError {
      DriverManager.getConnection(s"$dbUrl;shutdown=true")
    }
  }

  test("authenticate tests") {
    val providerImpl = new JdbcAuthenticationProviderImpl(conf)
    providerImpl.authenticate(authUser, authPasswd)

    val e1 = intercept[AuthenticationException] {
      providerImpl.authenticate("", "")
    }
    assert(e1.getMessage.contains("user is null"))

    val e4 = intercept[AuthenticationException] {
      providerImpl.authenticate(authUser, "wrong_password")
    }
    assert(e4.isInstanceOf[AuthenticationException])

    var _conf = conf.clone
    _conf.unset(AUTHENTICATION_JDBC_URL)
    val e5 = intercept[IllegalArgumentException] { new JdbcAuthenticationProviderImpl(_conf) }
    assert(e5.getMessage.contains("JDBC url is not configured"))

    _conf = conf.clone
    _conf.unset(AUTHENTICATION_JDBC_USERNAME)
    val e6 = intercept[IllegalArgumentException] { new JdbcAuthenticationProviderImpl(_conf) }
    assert(e6.getMessage.contains("JDBC username is not configured"))

    _conf = conf.clone
    _conf.unset(AUTHENTICATION_JDBC_QUERY)
    val e8 = intercept[IllegalArgumentException] { new JdbcAuthenticationProviderImpl(_conf) }
    assert(e8.getMessage.contains("Query SQL is not configured"))

    _conf.set(
      AUTHENTICATION_JDBC_QUERY,
      "INSERT INTO user_auth (username, password) VALUES ('demouser','demopassword');")
    val e9 = intercept[IllegalArgumentException] { new JdbcAuthenticationProviderImpl(_conf) }
    assert(e9.getMessage.contains("Query SQL must start with 'SELECT'"))

    _conf.unset(AUTHENTICATION_JDBC_URL)
    val e10 = intercept[IllegalArgumentException] { new JdbcAuthenticationProviderImpl(_conf) }
    assert(e10.getMessage.contains("JDBC url is not configured"))
  }
}
