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

package org.apache.kyuubi.operation

import java.sql.{DriverManager, SQLException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{KerberizedTestHelper, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{UserDefineAuthenticationProviderImpl, WithLdapServer}

class KyuubiOperationKerberosAndPlainAuthSuite extends
  WithKyuubiServer with KerberizedTestHelper with WithLdapServer with JDBCTestHelper {
  private val customUser: String = "user"
  private val customPasswd: String = "password"

  override protected def jdbcUrl: String = getJdbcUrl
  private def kerberosJdbcUrl: String = jdbcUrl + s"principal=${testPrincipal}"
  private val currentUser = UserGroupInformation.getCurrentUser

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty("java.security.krb5.conf")
    UserGroupInformation.setLoginUser(currentUser)
    UserGroupInformation.setConfiguration(new Configuration())
    assert(!UserGroupInformation.isSecurityEnabled)
    super.afterAll()
  }

  override protected lazy val conf: KyuubiConf = {
    val config = new Configuration()
    val authType = "hadoop.security.authentication"
    config.set(authType, "KERBEROS")
    System.setProperty("java.security.krb5.conf", krb5ConfPath)
    UserGroupInformation.setConfiguration(config)
    assert(UserGroupInformation.isSecurityEnabled)

    KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS", "LDAP", "CUSTOM"))
      .set(KyuubiConf.SERVER_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_PRINCIPAL, testPrincipal)
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASEDN, ldapBaseDn)
      .set(KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
  }

  test("test with KERBEROS authentication") {
    val conn = DriverManager.getConnection(jdbcUrlWithConf(kerberosJdbcUrl), user, "")
    try {
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery("select engine_name()")
      assert(resultSet.next())
      assert(resultSet.getString(1).nonEmpty)
    } finally {
      conn.close()
    }
  }

  test("test with LDAP authentication") {
    val conn = DriverManager.getConnection(jdbcUrlWithConf, ldapUser, ldapUserPasswd)
    try {
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery("select engine_name()")
      assert(resultSet.next())
      assert(resultSet.getString(1).nonEmpty)
    } finally {
      conn.close()
    }
  }

  test("only the first specified plain auth type is valid") {
    intercept[SQLException] {
      val conn = DriverManager.getConnection(jdbcUrlWithConf, customUser, customPasswd)
      try {
        val statement = conn.createStatement()
        statement.executeQuery("select engine_name()")
      } finally {
        conn.close()
      }
    }
  }

  test("test with invalid password") {
    intercept[SQLException] {
      val conn = DriverManager.getConnection(jdbcUrlWithConf, user, "invalidPassword")
      try {
        val statement = conn.createStatement()
        statement.executeQuery("select engine_name()")
      } finally {
        conn.close()
      }
    }
  }
}
