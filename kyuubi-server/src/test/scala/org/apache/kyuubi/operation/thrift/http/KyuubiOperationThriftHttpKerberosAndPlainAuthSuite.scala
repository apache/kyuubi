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

package org.apache.kyuubi.operation.thrift.http

import java.sql.{DriverManager, SQLException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.jdbc.hive.JdbcConnectionParams
import org.apache.kyuubi.operation.KyuubiOperationKerberosAndPlainAuthSuite
import org.apache.kyuubi.service.authentication.{UserDefineAuthenticationProviderImpl, UserDefineTokenAuthenticationProviderImpl}

class KyuubiOperationThriftHttpKerberosAndPlainAuthSuite
  extends KyuubiOperationKerberosAndPlainAuthSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    Thread.sleep(3000)
  }

  override protected val frontendProtocols: Seq[KyuubiConf.FrontendProtocols.Value] =
    FrontendProtocols.THRIFT_HTTP :: Nil

  override protected def kerberosTgtJdbcUrl: String =
    jdbcUrl.stripSuffix(";") + s";principal=$testSpnegoPrincipal"

  override protected def kerberosTgtJdbcUrlUsingAlias: String =
    jdbcUrl.stripSuffix(";") + s";kyuubiServerPrincipal=$testSpnegoPrincipal"

  override protected lazy val conf: KyuubiConf = {
    val config = new Configuration()
    val authType = "hadoop.security.authentication"
    config.set(authType, "KERBEROS")
    System.setProperty("java.security.krb5.conf", krb5ConfPath)
    UserGroupInformation.setConfiguration(config)
    assert(UserGroupInformation.isSecurityEnabled)

    KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS", "CUSTOM", "LDAP"))
      .set(KyuubiConf.SERVER_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_PRINCIPAL, testPrincipal)
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, ldapBaseDn.head)
      .set(
        KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
      .set(
        KyuubiConf.AUTHENTICATION_CUSTOM_BEARER_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
      .set(KyuubiConf.SERVER_SPNEGO_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_SPNEGO_PRINCIPAL, testSpnegoPrincipal)
  }

  override protected def getJdbcUrl: String =
    s"jdbc:hive2://${server.frontendServices.head.connectionUrl}/default;transportMode=http;" +
      s"httpPath=cliservice;"

  test("test with valid CUSTOM http bearer authentication") {
    withSessionConf(Map(JdbcConnectionParams.AUTH_TYPE_JWT_KEY
      -> UserDefineTokenAuthenticationProviderImpl.VALID_TOKEN))()() {
      val conn = DriverManager.getConnection(jdbcUrlWithConf)
      try {
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery("select engine_name()")
        assert(resultSet.next())
        assert(resultSet.getString(1).nonEmpty)
      } finally {
        conn.close()
      }
    }
  }

  test("test with invalid CUSTOM http bearer authentication") {
    withSessionConf(Map(JdbcConnectionParams.AUTH_TYPE_JWT_KEY -> "badToken"))()() {
      intercept[SQLException] {
        val conn = DriverManager.getConnection(jdbcUrlWithConf)
        try {
          val statement = conn.createStatement()
          statement.executeQuery("select engine_name()")
        } finally {
          conn.close()
        }
      }
    }
  }
}
