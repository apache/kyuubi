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

package org.apache.kyuubi.security.hive

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.security.HadoopDelegationTokenProvider

class HiveDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  private var issuer: HiveDelegationTokenIssuer = _

  override def serviceName: String = "hive"

  private def hiveConf(hadoopConf: Configuration): Configuration = {
    try {
      new HiveConf(hadoopConf, classOf[HiveConf])
    } catch {
      case NonFatal(e) =>
        warn("Fail to create Hive Configuration", e)
        hadoopConf
    }
  }

  override def delegationTokensRequired(hadoopConf: Configuration): Boolean = {
    val conf = hiveConf(hadoopConf)
    UserGroupInformation.isSecurityEnabled &&
    conf.getTrimmed("hive.metastore.uris", "").nonEmpty &&
    conf.getBoolean("hive.metastore.sasl.enabled", false)
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf,
      owner: String,
      creds: Credentials): Unit = {
    val conf = hiveConf(hadoopConf)

    val principalKey = "hive.metastore.kerberos.principal"
    val principal = conf.getTrimmed(principalKey, "")
    require(principal.nonEmpty, s"Hive principal $principalKey undefined")
    val metastoreUri = conf.getTrimmed("hive.metastore.uris", "")
    require(metastoreUri.nonEmpty, "Hive metastore uri undefined")

    debug(s"Getting Hive delegation token for $owner against $principal at $metastoreUri")

    if (issuer == null) {
      issuer = new HiveDelegationTokenIssuer(new HiveConf(conf, classOf[HiveConf]))
    }
    val tokenStr = issuer.getDelegationToken(owner, principal)
    val hive2Token = new Token[DelegationTokenIdentifier]()
    hive2Token.decodeFromUrlString(tokenStr)
    debug(s"Get Token from hive metastore: ${hive2Token.toString}")
    creds.addToken(tokenAlias, hive2Token)
  }

  private def tokenAlias: Text = new Text("hive.server2.delegation.token")
}
