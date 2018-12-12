/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.auth

import javax.security.auth.login.LoginException

import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authorize.AuthorizationException
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.service.ServiceException
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiAuthFactorySuite extends SparkFunSuite {

  test("testVerifyProxyAccess") {
    val conf = new SparkConf(true)
    val hadoopConf = KyuubiSparkUtil.newConfiguration(conf)
    val user = UserGroupInformation.getCurrentUser.getShortUserName
    KyuubiAuthFactory.verifyProxyAccess(user, user, "localhost", hadoopConf)
    val e = intercept[KyuubiSQLException](
      KyuubiAuthFactory.verifyProxyAccess(user, "proxy-user", "localhost", hadoopConf))
    val msg = "Failed to validate proxy privilege"
    assert(e.getMessage.contains(msg))
    assert(e.getCause.isInstanceOf[AuthorizationException])
    hadoopConf.set(s"hadoop.proxyuser.$user.groups", "*")
    val e2 = intercept[KyuubiSQLException](
      KyuubiAuthFactory.verifyProxyAccess(user, "proxy-user", "localhost", hadoopConf))
    assert(e2.getMessage.contains(msg))
    assert(e2.getCause.getMessage.contains("Unauthorized connection for super-user"))
    hadoopConf.set(s"hadoop.proxyuser.$user.hosts", "*")
    KyuubiAuthFactory.verifyProxyAccess(user, "proxy-user", "localhost", hadoopConf)
  }

  test("test HS2_PROXY_USER") {
    assert(KyuubiAuthFactory.HS2_PROXY_USER === "hive.server2.proxy.user")
  }

  test("AuthType NONE") {
    val conf = new SparkConf(true)
    KyuubiSparkUtil.setupCommonConfig(conf)
    val auth = new KyuubiAuthFactory(conf)
    val saslServer = ReflectUtils.getFieldValue(auth, "saslServer")
    assert(saslServer === None)
    assert(auth.getRemoteUser === None)
    assert(auth.getIpAddress === None)
    val user = UserGroupInformation.getCurrentUser.getShortUserName
    val e = intercept[KyuubiSQLException](auth.getDelegationToken(user, user))
    assert(e.getMessage.contains("Delegation token only supported over kerberos authentication"))
    assert(e.toTStatus.getSqlState === "08S01")
    val e1 = intercept[KyuubiSQLException](auth.cancelDelegationToken(""))
    assert(e1.getMessage.contains("Delegation token only supported over kerberos authentication"))
    assert(e1.toTStatus.getSqlState === "08S01")
    val e2 = intercept[KyuubiSQLException](auth.renewDelegationToken(""))
    assert(e2.getMessage.contains("Delegation token only supported over kerberos authentication"))
    assert(e2.toTStatus.getSqlState === "08S01")
  }

  test("AuthType Other") {
    val conf = new SparkConf(true).set(KyuubiConf.AUTHENTICATION_METHOD.key, "other")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val e = intercept[ServiceException](new KyuubiAuthFactory(conf))
    assert(e.getMessage === "Unsupported authentication method: OTHER")
  }

  test("AuthType LDAP") {
    val conf = new SparkConf(true).set(KyuubiConf.AUTHENTICATION_METHOD.key, "LDAP")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val authFactory = new KyuubiAuthFactory(conf)
    assert(authFactory.getIpAddress.isEmpty)
  }

  test("AuthType KERBEROS without keytab/principal") {
    val conf = new SparkConf(true).set(KyuubiConf.AUTHENTICATION_METHOD.key, "KERBEROS")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val e = intercept[ServiceException](new KyuubiAuthFactory(conf))
    assert(e.getMessage === "spark.yarn.keytab and spark.yarn.principal are not configured " +
      "properly for KERBEROS Authentication method")
  }

  test("AuthType KERBEROS with keytab/principal ioe") {
    val conf = new SparkConf(true)
      .set(KyuubiConf.AUTHENTICATION_METHOD.key, "KERBEROS")
        .set(KyuubiSparkUtil.KEYTAB, "kent.keytab")
        .set(KyuubiSparkUtil.PRINCIPAL, "kent")
    KyuubiSparkUtil.setupCommonConfig(conf)
    val auth = new KyuubiAuthFactory(conf)
    val saslServer = ReflectUtils.getFieldValue(auth, "saslServer")
    saslServer match {
      case Some(server) =>
        assert(server.isInstanceOf[HadoopThriftAuthBridge.Server])
        intercept[LoginException](auth.getAuthTransFactory)
      case None => assert(false, "server could not be none")
    }

    val user = UserGroupInformation.getCurrentUser.getShortUserName
    val e = intercept[KyuubiSQLException](auth.getDelegationToken(user, user))
    assert(e.getMessage.contains(s"Error retrieving delegation token for user $user"))
    assert(e.toTStatus.getSqlState === "08S01")
    val e1 = intercept[KyuubiSQLException](auth.cancelDelegationToken(""))
    assert(e1.getMessage.contains("Error canceling delegation token"))
    assert(e1.toTStatus.getSqlState === "08S01")
    val e2 = intercept[KyuubiSQLException](auth.renewDelegationToken(""))
    assert(e2.getMessage.contains("Error renewing delegation token"))
    assert(e2.toTStatus.getSqlState === "08S01")
  }

}
