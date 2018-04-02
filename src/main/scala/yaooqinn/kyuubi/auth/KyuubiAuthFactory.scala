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
import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.spark.{SparkConf, SparkUtils}
import org.apache.spark.KyuubiConf._
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.transport.{TServerSocket, TTransportException, TTransportFactory}

import yaooqinn.kyuubi.{KyuubiException, KyuubiSQLException}

/**
 * Authentication
 */
class KyuubiAuthFactory(conf: SparkConf) {
  private[this] val KYUUBI_CLIENT_TOKEN = "kyuubiClientToken"

  private[this] val saslServer: Option[HadoopThriftAuthBridge.Server] =
    conf.get(AUTHENTICATION_METHOD.key).toUpperCase match {
      case KERBEROS.name =>
        val principal: String = conf.get("spark.yarn.principal", "")
        val keytab: String = conf.get("spark.yarn.keytab", "")
        require(principal.nonEmpty && keytab.nonEmpty,
          "keytab and principal are not configured properly")
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
        Some(server)
      case NONE.name => None
      case other => throw new KyuubiException("Unsupported authentication method: " + other)
    }

  private[this] def getSaslProperties: Map[String, String] = {
    Map(
      Sasl.QOP -> SaslQOP.fromString(conf.get(SASL_QOP.key)).toString,
      Sasl.SERVER_AUTH -> "true")
  }

  @throws[LoginException]
  def getAuthTransFactory: TTransportFactory = saslServer match {
    case Some(server) =>
      try {
        server.createTransportFactory(getSaslProperties.asJava)
      } catch {
        case e: TTransportException =>
          throw new LoginException(e.getMessage)
      }
    case _ => KyuubiPlainSaslHelper.getPlainTransportFactory(NONE.name)
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   */
  def getAuthProcFactory(
      service: TCLIService.Iface): TProcessorFactory = saslServer.map {
    KyuubiKerberosSaslHelper.getKerberosProcessorFactory(_, service)
  }.getOrElse {
    KyuubiPlainSaslHelper.getPlainProcessorFactory(service)
  }

  def getRemoteUser: Option[String] = saslServer.map(_.getRemoteUser)

  def getIpAddress: Option[String] = saslServer.map(_.getRemoteAddress).map(_.getHostAddress)

  // retrieve delegation token for the given user
  @throws[KyuubiSQLException]
  def getDelegationToken(owner: String, renewer: String): String = saslServer match {
    case Some(server) =>
      try {
        val tokenStr = server.getDelegationTokenWithService(owner, renewer, KYUUBI_CLIENT_TOKEN)
        if (tokenStr == null || tokenStr.isEmpty) {
          throw new KyuubiSQLException(
            "Received empty retrieving delegation token for user " + owner, "08S01")
        }
        tokenStr
      } catch {
        case e: IOException =>
          throw new KyuubiSQLException(
            "Error retrieving delegation token for user " + owner, "08S01", e)
        case e: InterruptedException =>
          throw new KyuubiSQLException("delegation token retrieval interrupted", "08S01", e)
      }
    case None =>
      throw new KyuubiSQLException(
        "Delegation token only supported over kerberos authentication", "08S01")
  }

  // cancel given delegation token
  @throws[KyuubiSQLException]
  def cancelDelegationToken(delegationToken: String): Unit = saslServer match {
    case Some(server) =>
      try {
        server.cancelDelegationToken(delegationToken)
      } catch {
        case e: IOException =>
          throw new KyuubiSQLException(
            "Error canceling delegation token " + delegationToken, "08S01", e)
      }
    case None =>
      throw new KyuubiSQLException(
        "Delegation token only supported over kerberos authentication", "08S01")
  }

  @throws[KyuubiSQLException]
  def renewDelegationToken(delegationToken: String): Unit = saslServer match {
    case Some(server) =>
      try {
        server.renewDelegationToken(delegationToken)
      } catch {
        case e: IOException =>
          throw new KyuubiSQLException(
            "Error renewing delegation token " + delegationToken, "08S01", e)
      }
    case None =>
      throw new KyuubiSQLException(
        "Delegation token only supported over kerberos authentication", "08S01")
  }
}

object KyuubiAuthFactory {
  val HS2_PROXY_USER = "hive.server2.proxy.user"
  @throws[TTransportException]
  def getServerSocket(hiveHost: String, portNum: Int): TServerSocket = new TServerSocket(
    if (hiveHost == null || hiveHost.isEmpty) {
      // Wildcard bind
      new InetSocketAddress(portNum)
    } else {
      new InetSocketAddress(hiveHost, portNum)
    }
  )

  @throws[KyuubiSQLException]
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
        throw new KyuubiSQLException(
          "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e)
    }
  }
}
