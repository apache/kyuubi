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

import java.security.Security
import javax.security.auth.login.LoginException

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.PlainSASLServer.SaslPlainProvider
import org.apache.kyuubi.util.KyuubiHadoopUtils

class KyuubiAuthenticationFactorySuite extends KyuubiFunSuite {
  import KyuubiAuthenticationFactory._

  test("verify proxy access") {
    val kyuubiConf = KyuubiConf()
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)

    val e1 = intercept[KyuubiSQLException] {
      verifyProxyAccess("kent", "yao", "localhost", hadoopConf)
    }
    assert(e1.getMessage === "Failed to validate proxy privilege of kent for yao")

    kyuubiConf.set("hadoop.proxyuser.kent.groups", "*")
    kyuubiConf.set("hadoop.proxyuser.kent.hosts", "*")
    val hadoopConf2 = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    verifyProxyAccess("kent", "yao", "localhost", hadoopConf2)
  }

  test("AuthType NONE") {
    val kyuubiConf = KyuubiConf()
    val auth = new KyuubiAuthenticationFactory(kyuubiConf)
    auth.getTTransportFactory
    assert(Security.getProviders.exists(_.isInstanceOf[SaslPlainProvider]))

    assert(auth.getIpAddress.isEmpty)
    assert(auth.getRemoteUser.isEmpty)
  }

  test("AuthType Other") {
    val conf = KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("INVALID"))
    val e = intercept[IllegalArgumentException](new KyuubiAuthenticationFactory(conf))
    assert(e.getMessage === "the authentication type should be one or more of" +
      " NOSASL,NONE,LDAP,KERBEROS,CUSTOM")
  }

  test("AuthType LDAP") {
    val conf = KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("LDAP"))
    val authFactory = new KyuubiAuthenticationFactory(conf)
    authFactory.getTTransportFactory
    assert(Security.getProviders.exists(_.isInstanceOf[SaslPlainProvider]))
  }


  test("AuthType KERBEROS w/o keytab/principal") {
    val conf = KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS"))

    val factory = new KyuubiAuthenticationFactory(conf)
    val e = intercept[LoginException](factory.getTTransportFactory)
    assert(e.getMessage startsWith "Kerberos principal should have 3 parts")
  }
}
