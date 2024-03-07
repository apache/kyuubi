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
import org.apache.hadoop.security.token.Token

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.shaded.hive.metastore.{IMetaStoreClient, RetryingMetaStoreClient}
import org.apache.kyuubi.shaded.hive.metastore.conf.MetastoreConf
import org.apache.kyuubi.shaded.hive.metastore.conf.MetastoreConf.ConfVars
import org.apache.kyuubi.shaded.hive.metastore.security.DelegationTokenIdentifier

class HiveDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  private var client: Option[IMetaStoreClient] = None
  private var principal: String = _
  private var tokenAlias: Text = _

  override def serviceName: String = "hive"

  override def initialize(hadoopConf: Configuration, kyuubiConf: KyuubiConf): Unit = {
    val conf = MetastoreConf.newMetastoreConf(hadoopConf)
    val metastoreUris = MetastoreConf.getVar(hadoopConf, ConfVars.THRIFT_URIS)
    // SQL engine requires token alias to be `hive.metastore.uris`
    tokenAlias = new Text(metastoreUris)

    if (SecurityUtil.getAuthenticationMethod(hadoopConf) != AuthenticationMethod.SIMPLE
      && metastoreUris.nonEmpty
      && MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL)) {

      principal = MetastoreConf.getVar(hadoopConf, ConfVars.KERBEROS_PRINCIPAL)
      require(
        principal.nonEmpty,
        s"Hive principal ${ConfVars.KERBEROS_PRINCIPAL.getVarname} undefined")

      client = Some(RetryingMetaStoreClient.getProxy(conf))
      info(s"Created HiveMetaStoreClient with metastore uris $metastoreUris")
    }
  }

  override def delegationTokensRequired(): Boolean = client.nonEmpty

  override def obtainDelegationTokens(owner: String, creds: Credentials): Unit = {
    client.foreach { client =>
      info(s"Getting Hive delegation token for $owner against $principal")
      val tokenStr = client.getDelegationToken(owner, principal)
      val hive2Token = new Token[DelegationTokenIdentifier]()
      hive2Token.decodeFromUrlString(tokenStr)
      debug(s"Get Token from hive metastore: ${hive2Token.toString}")
      creds.addToken(tokenAlias, hive2Token)
    }
  }

  override def close(): Unit = client.foreach(_.close())
}
