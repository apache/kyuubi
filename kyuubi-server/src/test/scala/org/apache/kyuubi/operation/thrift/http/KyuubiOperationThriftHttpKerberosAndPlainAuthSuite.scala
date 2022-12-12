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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.operation.KyuubiOperationKerberosAndPlainAuthSuite
import org.apache.kyuubi.service.authentication.UserDefineAuthenticationProviderImpl

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

    KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS", "LDAP", "CUSTOM"))
      .set(KyuubiConf.SERVER_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_PRINCIPAL, testPrincipal)
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASEDN, ldapBaseDn)
      .set(
        KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
      .set(KyuubiConf.SERVER_SPNEGO_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_SPNEGO_PRINCIPAL, testSpnegoPrincipal)
  }

  override protected def getJdbcUrl: String =
    s"jdbc:hive2://${server.frontendServices.head.connectionUrl}/default;transportMode=http;" +
      s"httpPath=cliservice"
}
