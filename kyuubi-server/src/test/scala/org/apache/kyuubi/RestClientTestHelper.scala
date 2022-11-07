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

package org.apache.kyuubi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{UserDefineAuthenticationProviderImpl, WithLdapServer}

trait RestClientTestHelper extends RestFrontendTestHelper with KerberizedTestHelper
  with WithLdapServer {

  protected val customUser: String = "user"
  protected val customPasswd: String = "password"
  private val currentUser = UserGroupInformation.getCurrentUser

  override def afterAll(): Unit = {
    System.clearProperty("java.security.krb5.conf")
    UserGroupInformation.setLoginUser(currentUser)
    UserGroupInformation.setConfiguration(new Configuration())
    assert(!UserGroupInformation.isSecurityEnabled)
    super.afterAll()
  }

  protected val otherConfigs: Map[String, String] = Map.empty

  override protected lazy val conf: KyuubiConf = {
    val config = new Configuration()
    val authType = "hadoop.security.authentication"
    config.set(authType, "KERBEROS")
    System.setProperty("java.security.krb5.conf", krb5ConfPath)
    UserGroupInformation.setConfiguration(config)
    assert(UserGroupInformation.isSecurityEnabled)

    val conf = KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS", "LDAP", "CUSTOM"))
      .set(KyuubiConf.SERVER_KEYTAB.key, testKeytab)
      .set(KyuubiConf.SERVER_PRINCIPAL, testPrincipal)
      .set(KyuubiConf.SERVER_SPNEGO_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_SPNEGO_PRINCIPAL, testSpnegoPrincipal)
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASEDN, ldapBaseDn)
      .set(
        KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
    otherConfigs.foreach(kv => conf.set(kv._1, kv._2))
    conf
  }

}
