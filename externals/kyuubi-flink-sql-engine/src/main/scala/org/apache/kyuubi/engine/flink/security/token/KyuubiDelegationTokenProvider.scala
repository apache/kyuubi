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

package org.apache.kyuubi.engine.flink.security.token

import java.util
import java.util.{HashMap, Optional}

import scala.collection.JavaConverters._

import org.apache.flink.client.deployment.application.ApplicationConfiguration.APPLICATION_ARGS
import org.apache.flink.configuration.{Configuration, ConfigUtils}
import org.apache.flink.core.security.token.DelegationTokenProvider
import org.apache.flink.runtime.security.token.hadoop.HadoopDelegationTokenConverter
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.engine.flink.FlinkEngineUtils
import org.apache.kyuubi.util.KyuubiHadoopUtils

class KyuubiDelegationTokenProvider extends DelegationTokenProvider with Logging {

  override def serviceName(): String = "kyuubi"

  @volatile private var previousTokens: util.Map[Text, Token[_ <: TokenIdentifier]] = _
  private var renewalInterval: Long = _

  override def init(configuration: Configuration): Unit = {
    val programArgsList = ConfigUtils
      .decodeListFromConfig(configuration, APPLICATION_ARGS, (t: String) => new String(t))
    val kyuubiConf: KyuubiConf = KyuubiConf(false)
    Utils.fromCommandLineArgs(programArgsList.toArray(new Array[String](0)), kyuubiConf)
    val engineCredentials = kyuubiConf.getOption(KyuubiReservedKeys.KYUUBI_ENGINE_CREDENTIALS_KEY)
    if (engineCredentials.isDefined) {
      info(s"Renew delegation token with engine credentials: $engineCredentials")
      FlinkEngineUtils.renewDelegationToken(engineCredentials.get)
    }
    val credentials: Credentials = UserGroupInformation.getCurrentUser.getCredentials
    previousTokens = new util.HashMap[Text, Token[_ <: TokenIdentifier]](credentials.getTokenMap)
    renewalInterval = kyuubiConf.get(KyuubiConf.CREDENTIALS_RENEWAL_INTERVAL)
  }

  override def delegationTokensRequired(): Boolean = true

  override def obtainDelegationTokens(): DelegationTokenProvider.ObtainedDelegationTokens = {
    // Maybe updated by `FlinkTBinaryFrontendService.RenewDelegationToken`
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val newCredentials = new Credentials

    credentials.getTokenMap.asScala.foreach { case (alias, token) =>
      val previousToken = previousTokens.get(alias)
      if (previousToken == null || KyuubiHadoopUtils.compareIssueDate(token, previousToken) > 0) {
        newCredentials.addToken(alias, token)
      }
    }

    previousTokens = new HashMap[Text, Token[_ <: TokenIdentifier]](credentials.getTokenMap)
    val validUntil = Optional.of[java.lang.Long](System.currentTimeMillis + renewalInterval)
    new DelegationTokenProvider.ObtainedDelegationTokens(
      HadoopDelegationTokenConverter.serialize(credentials),
      validUntil)
  }
}
