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

package org.apache.kyuubi.credentials

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.yarn.client.ClientRMProxy
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_EXTERNAL_TOKEN_ENABLED
import org.apache.kyuubi.credentials.HadoopFsDelegationTokenProvider.doAsProxyUser

class YarnRMDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {
  private var yarnConf: YarnConfiguration = _
  private var tokenService: Text = _
  private var required = false
  override def serviceName: String = "yarn"

  def getTokenService(): Text = tokenService

  // Only support engine and kyuubi server using same hadoop conf
  override def initialize(hadoopConf: Configuration, kyuubiConf: KyuubiConf): Unit = {
    if (SecurityUtil.getAuthenticationMethod(hadoopConf) != AuthenticationMethod.SIMPLE) {
      yarnConf = new YarnConfiguration(hadoopConf)
      tokenService = ClientRMProxy.getRMDelegationTokenService(yarnConf)
      required = kyuubiConf.get(ENGINE_EXTERNAL_TOKEN_ENABLED)
    }
  }

  override def delegationTokensRequired(): Boolean = required

  override def obtainDelegationTokens(owner: String, creds: Credentials): Unit = {
    doAsProxyUser(owner) {
      var client: Option[YarnClient] = None
      try {
        client = Some(YarnClient.createYarnClient())
        client.foreach(client => {
          client.init(yarnConf)
          client.start()
          val yarnToken = ConverterUtils.convertFromYarn(
            client.getRMDelegationToken(new Text()),
            tokenService)
          info(s"Get Token from Resource Manager service ${tokenService}, " +
            s"token : ${yarnToken.toString}")
          creds.addToken(new Text(yarnToken.getService), yarnToken)
        })
      } catch {
        case e: Throwable => error("Error occurs when get delegation token", e)
      } finally {
        client.foreach(_.close())
      }
    }
  }
}
