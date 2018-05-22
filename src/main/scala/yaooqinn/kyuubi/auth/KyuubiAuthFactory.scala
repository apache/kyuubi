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
import javax.security.auth.login.LoginException
import javax.security.sasl.Sasl

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authorize.ProxyUsers
import org.apache.hive.service.cli.thrift.TCLIService
import org.apache.spark.{SparkConf, KyuubiSparkUtil}
import org.apache.spark.KyuubiConf._
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.transport.{TTransportException, TTransportFactory}

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.service.ServiceException

/**
 * Authentication
 */
class KyuubiAuthFactory(conf: SparkConf) extends Logging {
  private[this] val KYUUBI_CLIENT_TOKEN = "kyuubiClientToken"
  private[this] val authMethod = AuthType.toAuthType(conf.get(AUTHENTICATION_METHOD.key))
  private[this] val saslServer: Option[HadoopThriftAuthBridge.Server] = authMethod match {
    case AuthType.KERBEROS =>
      val principal: String = conf.get(KyuubiSparkUtil.PRINCIPAL, "")
      val keytab: String = conf.get(KyuubiSparkUtil.KEYTAB, "")
      if (principal.isEmpty || keytab.isEmpty) {
        val msg = s"${KyuubiSparkUtil.KEYTAB} and ${KyuubiSparkUtil.PRINCIPAL} are not configured" +
          s" properly for ${AuthType.KERBEROS} Authentication method"
        throw new ServiceException(msg)
      }
      val server = ShimLoader.getHadoopThriftAuthBridge.createServer(keytab, principal)
      info("Starting Kyuubi client token manager")
      try {
        server.startDelegationTokenSecretManager(
          KyuubiSparkUtil.newConfiguration(conf),
          null,
          ServerMode.HIVESERVER2)
      } catch {
        case e: IOException =>
          throw new TTransportException("Failed to start token manager", e)
      }
      Some(server)
    case AuthType.NONE | AuthType.NOSASL => None
    case other => throw new ServiceException("Unsupported authentication method: " + other)
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
    case _ => authMethod match {
      case AuthType.NOSASL => new TTransportFactory
      case _ => PlainSaslHelper.getTransportFactory(authMethod.name)
    }
  }

  /**
   * Returns the thrift processor factory for Kyuubi running in binary mode
   */
  def getAuthProcFactory(service: TCLIService.Iface): TProcessorFactory = saslServer.map {
    KerberosSaslHelper.getProcessorFactory(_, service)
  }.getOrElse {
    PlainSaslHelper.getProcessFactory(service)
  }

  def getRemoteUser: Option[String] = saslServer.map(_.getRemoteUser)

  def getIpAddress: Option[String] = {
    saslServer.map(_.getRemoteAddress).filter(_ != null).map(_.getHostAddress)
  }

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

  @throws[KyuubiSQLException]
  def verifyProxyAccess(
      realUser: String,
      proxyUser: String,
      ipAddress: String,
      hadoopConf: Configuration): Unit = {
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
        ProxyUsers.refreshSuperUserGroupsConfiguration(hadoopConf)
        ProxyUsers.authorize(
          UserGroupInformation.createProxyUser(proxyUser, sessionUgi),
          ipAddress,
          hadoopConf)
      }
    } catch {
      case e: IOException =>
        throw new KyuubiSQLException(
          "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e)
    }
  }
}
