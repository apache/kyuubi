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

import java.nio.file.Path
import java.sql.{Connection, DriverManager}
import java.util.Properties
import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class JdbcAuthenticationProviderImplSuite extends KyuubiFunSuite {
  protected val dbUser: String = "bowenliang123"
  protected val dbPasswd: String = "bowenliang123"
  protected var jdbcUrl: String = _

  protected val authUser: String = "liangtiancheng"
  protected val authPasswd: String = "liangtiancheng"

  protected var conf = new KyuubiConf()
  var conn: Connection = _
  var authDb: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val datasourceProperties = new Properties()
    datasourceProperties.put("user", dbUser)
    datasourceProperties.put("password", dbPasswd)

    authDb = Utils.createTempDir(namePrefix = getClass.getSimpleName)
    authDb.toFile.delete()

    jdbcUrl = s"jdbc:derby:;databaseName=$authDb;create=true"
    conn = DriverManager.getConnection(
      jdbcUrl
        + ";user=" + dbUser
        + ";password=" + dbPasswd,
      datasourceProperties)

    conn.prepareStatement("CREATE SCHEMA " + dbUser).execute();

    conn.prepareStatement("CREATE TABLE user_auth (" +
      "username VARCHAR(64) NOT NULL PRIMARY KEY, " +
      "passwd VARCHAR(64))").execute();

    val insertStmt = conn.prepareStatement("INSERT INTO user_auth " +
      "(username, passwd) VALUES (?,?)")
    insertStmt.setString(1, authUser)
    insertStmt.setString(2, authPasswd)
    insertStmt.execute();

    conf = genJdbcAuthConfigs
  }

  override def afterAll(): Unit = {
    super.afterAll()

    // shutdown derby database
    try {
      DriverManager.getConnection(s"jdbc:derby:;databaseName=$authDb;shutdown=true")
    } catch {
      case e: Throwable =>
    }
  }

  test("authenticate tests") {
    var providerImpl = new JdbcAuthenticationProviderImpl(conf)

    providerImpl.authenticate(authUser, authPasswd)

    val e1 = intercept[AuthenticationException](providerImpl.authenticate("", ""))
    assert(e1.getMessage.contains("user is null"))

    val e2 = intercept[AuthenticationException](providerImpl.authenticate("kyuubi", ""))
    assert(e2.getMessage.contains("password is null"))

    val e4 = intercept[AuthenticationException](
      providerImpl.authenticate(authPasswd, "pass"))
    assert(e4.isInstanceOf[AuthenticationException])

    conf = genJdbcAuthConfigs
    conf.unset(AUTHENTICATION_JDBC_URL)
    providerImpl = new JdbcAuthenticationProviderImpl(conf)
    val e5 = intercept[IllegalArgumentException](providerImpl.authenticate(authUser, authPasswd))
    assert(e5.getMessage.contains("JDBC url is not configured"))

    conf = genJdbcAuthConfigs
    conf.unset(AUTHENTICATION_JDBC_USERNAME)
    providerImpl = new JdbcAuthenticationProviderImpl(conf)
    val e6 = intercept[IllegalArgumentException](providerImpl.authenticate(authUser, authPasswd))
    assert(e6.getMessage.contains("JDBC username or password is not configured"))

    conf = genJdbcAuthConfigs
    conf.unset(AUTHENTICATION_JDBC_PASSWORD)
    providerImpl = new JdbcAuthenticationProviderImpl(conf)
    val e7 = intercept[IllegalArgumentException](providerImpl.authenticate(authUser, authPasswd))
    assert(e7.getMessage.contains("JDBC username or password is not configured"))

    conf = genJdbcAuthConfigs
    conf.unset(AUTHENTICATION_JDBC_QUERY)
    providerImpl = new JdbcAuthenticationProviderImpl(conf)
    val e8 = intercept[IllegalArgumentException](providerImpl.authenticate(authUser, authPasswd))
    assert(e8.getMessage.contains("Query SQL is not configured"))

    conf.set(
      AUTHENTICATION_JDBC_QUERY,
      "INSERT INTO user_auth (username, password) " +
        " VALUES ('demouser','demopassword'); ")
    providerImpl = new JdbcAuthenticationProviderImpl(conf)
    val e9 = intercept[IllegalArgumentException](providerImpl.authenticate(authUser, authPasswd))
    assert(e9.getMessage.contains("Query SQL must start with \"SELECT\""))

    conf.unset(AUTHENTICATION_JDBC_URL)
    providerImpl = new JdbcAuthenticationProviderImpl(conf)
    val e10 = intercept[IllegalArgumentException](providerImpl.authenticate(authUser, authPasswd))
    assert(e10.getMessage.contains("JDBC url is not configured"))
  }

  private def genJdbcAuthConfigs: KyuubiConf = {
    conf = new KyuubiConf()
    conf.set(AUTHENTICATION_JDBC_DRIVER, "org.apache.derby.jdbc.AutoloadedDriver")
    conf.set(AUTHENTICATION_JDBC_URL, jdbcUrl)
    conf.set(AUTHENTICATION_JDBC_USERNAME, dbUser)
    conf.set(AUTHENTICATION_JDBC_PASSWORD, dbPasswd)
    conf.set(
      AUTHENTICATION_JDBC_QUERY,
      "SELECT 1 FROM user_auth " +
        " WHERE username=${username} and passwd=${password}")
    conf
  }
}
