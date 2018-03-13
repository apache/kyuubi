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

import java.io.IOException
import java.net.InetSocketAddress
import javax.security.auth.login.LoginException
import javax.security.sasl.Sasl

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authorize.ProxyUsers
import org.apache.hive.service.auth.{KyuubiKerberosSaslHelper, KyuubiPlainSaslHelper, SaslQOP}
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.spark.{SparkConf, SparkUtils}
import org.apache.spark.KyuubiConf._
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.transport.{TServerSocket, TTransportException, TTransportFactory}

/**
 * Authentication
 */
class KyuubiAuthFactory(conf: SparkConf) {

  object AuthTypes extends Enumeration {
    val NONE, KERBEROS = Value
  }

  private[this] val principal: String = conf.get(PRINCIPAL.key, "")
  private[this] val keytab: String = conf.get(KEYTAB.key, "")
  private[this] val authTypeStr: String =
    if (conf.get(AUTHENTICATION_METHOD.key) == null || keytab.isEmpty || principal.isEmpty ) {
      AuthTypes.NONE.toString
    } else {
      conf.get(AUTHENTICATION_METHOD.key)
    }

  private var saslServer: HadoopThriftAuthBridge.Server =
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.toString)) {
      val server = ShimLoader.getHadoopThriftAuthBridge.createServer(keytab, principal)
      // start delegation token manager
      try {
        server.startDelegationTokenSecretManager(
          SparkUtils.newConfiguration(conf),
          null,
          ServerMode.HIVESERVER2)
      } catch {
        case e: IOException =>
          throw new TTransportException("Failed to start token manager", e)
      }
      server
    } else {
      null
    }

  val HS2_PROXY_USER = "hive.server2.proxy.user"
  val KYUUBI_CLIENT_TOKEN = "kyuubiClientToken"

  private[this] def getSaslProperties: Map[String, String] = {
    Map(
      Sasl.QOP -> SaslQOP.fromString(conf.get(SASL_QOP.key)).toString,
      Sasl.SERVER_AUTH -> "true")
  }


  @throws[LoginException]
  def getAuthTransFactory: TTransportFactory = {
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.toString)) {
      try {
        saslServer.createTransportFactory(getSaslProperties.asJava)
      } catch {
        case e: TTransportException =>
          throw new LoginException(e.getMessage)
      }
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NONE.toString)) {
       KyuubiPlainSaslHelper.getPlainTransportFactory(authTypeStr)
    } else {
      throw new LoginException("Unsupported authentication type " + authTypeStr)
    }
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   *
   * @param service
   *
   * @return
   */
  def getAuthProcFactory(
      service: TCLIService.Iface): TProcessorFactory =
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.toString)) {
      KyuubiKerberosSaslHelper.getKerberosProcessorFactory(saslServer, service)
    } else {
      KyuubiPlainSaslHelper.getPlainProcessorFactory(service)
    }

  def getRemoteUser: String = if (saslServer == null) {
    null
  } else {
    saslServer.getRemoteUser
  }

  def getIpAddress: String = if (saslServer == null || saslServer.getRemoteAddress == null) {
    null
  } else {
    saslServer.getRemoteAddress.getHostAddress
  }

  // retrieve delegation token for the given user
  @throws[HiveSQLException]
  def getDelegationToken(owner: String, renewer: String): String = {
    if (saslServer == null) {
      throw new HiveSQLException(
        "Delegation token only supported over kerberos authentication", "08S01")
    }
    try {
      val tokenStr = saslServer.getDelegationTokenWithService(owner, renewer, KYUUBI_CLIENT_TOKEN)
      if (tokenStr == null || tokenStr.isEmpty) {
        throw new HiveSQLException(
          "Received empty retrieving delegation token for user " + owner, "08S01")
      }
      tokenStr
    } catch {
      case e: IOException =>
        throw new HiveSQLException(
          "Error retrieving delegation token for user " + owner, "08S01", e)
      case e: InterruptedException =>
        throw new HiveSQLException("delegation token retrieval interrupted", "08S01", e)
    }
  }

  // cancel given delegation token
  @throws[HiveSQLException]
  def cancelDelegationToken(delegationToken: String): Unit = {
    if (saslServer == null) {
      throw new HiveSQLException(
        "Delegation token only supported over kerberos authentication", "08S01")
    }
    try {
      saslServer.cancelDelegationToken(delegationToken)
    } catch {
      case e: IOException =>
        throw new HiveSQLException(
          "Error canceling delegation token " + delegationToken, "08S01", e)
    }
  }

  @throws[HiveSQLException]
  def renewDelegationToken(delegationToken: String): Unit = {
    if (saslServer == null) {
      throw new HiveSQLException(
        "Delegation token only supported over kerberos authentication", "08S01")
    }
    try {
      saslServer.renewDelegationToken(delegationToken)
    }
    catch {
      case e: IOException =>
        throw new HiveSQLException("Error renewing delegation token " + delegationToken, "08S01", e)
    }
  }
}

object KyuubiAuthFactory {

  @throws[TTransportException]
  def getServerSocket(hiveHost: String, portNum: Int): TServerSocket = {
    new TServerSocket(
      if (hiveHost == null || hiveHost.isEmpty) {
      // Wildcard bind
      new InetSocketAddress(portNum)
      } else {
        new InetSocketAddress(hiveHost, portNum)
      }
    )
  }

  @throws[HiveSQLException]
  def verifyProxyAccess(
      realUser: String,
      proxyUser: String,
      ipAddress: String,
      hiveConf: HiveConf): Unit = {
    try {
      val sessionUgi = {
        if (UserGroupInformation.isSecurityEnabled) {
          val kerbName = ShimLoader.getHadoopShims.getKerberosNameShim(realUser)
          UserGroupInformation.createProxyUser(
            kerbName.getServiceName,
            UserGroupInformation.getLoginUser)
        } else {
          UserGroupInformation.createRemoteUser(realUser)
        }
      }

      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration(hiveConf)
        ProxyUsers.authorize(
          UserGroupInformation.createProxyUser(proxyUser, sessionUgi),
          ipAddress,
          hiveConf)
      }
    } catch {
      case e: IOException =>
        throw new HiveSQLException(
          "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e)
    }
  }
}
