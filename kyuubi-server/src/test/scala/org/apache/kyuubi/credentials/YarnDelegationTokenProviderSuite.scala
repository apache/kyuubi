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

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier

import org.apache.kyuubi.WithSecuredYarnCluster
import org.apache.kyuubi.config.KyuubiConf

class YarnDelegationTokenProviderSuite extends WithSecuredYarnCluster {

  test("obtain yarn rm delegation tokens") {
    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)

      val hadoopConf = getHadoopConf
      val kyuubiConf = new KyuubiConf(false)
      kyuubiConf.set("kyuubi.engine.external.token.enabled", "true")

      val provider = new YarnRMDelegationTokenProvider
      provider.initialize(hadoopConf, kyuubiConf)
      assert(provider.delegationTokensRequired())

      val owner = "who"
      val credentials = new Credentials()
      provider.obtainDelegationTokens(owner, credentials)

      val token = credentials
        .getToken(provider.getTokenService())
        .asInstanceOf[Token[AbstractDelegationTokenIdentifier]]
      assert(token != null)

      val tokenIdent = token.decodeIdentifier()
      assertResult(RMDelegationTokenIdentifier.KIND_NAME)(token.getKind)
      assertResult(new Text(owner))(tokenIdent.getOwner)
      val currentUserName = UserGroupInformation.getCurrentUser.getUserName
      assertResult(new Text(currentUserName))(tokenIdent.getRealUser)
    }
  }
}
