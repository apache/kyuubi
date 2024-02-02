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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hadoop.security.authorize.ProxyUsers

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.service.authentication.AuthTypes.{AuthType, KERBEROS, NOSASL}

object AuthUtils extends Logging {
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
          val kerbName = new KerberosName(realUser)
          UserGroupInformation.createProxyUser(
            kerbName.getServiceName,
            UserGroupInformation.getLoginUser)
        } else {
          UserGroupInformation.createRemoteUser(realUser)
        }
      }

      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration(hadoopConf)
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, sessionUgi), ipAddress)
      }
    } catch {
      case e: IOException =>
        throw KyuubiSQLException(
          "Failed to validate proxy privilege of " + realUser + " for " + proxyUser,
          e)
    }
  }

  def saslDisabled(authTypes: Seq[AuthType]): Boolean = authTypes == Seq(NOSASL)

  def kerberosEnabled(authTypes: Seq[AuthType]): Boolean = authTypes.contains(KERBEROS)

  // take the first declared SASL/PLAIN auth type
  def effectivePlainAuthType(authTypes: Seq[AuthType]): Option[AuthType] = authTypes.find {
    case NOSASL | KERBEROS => false
    case _ => true
  }
}
