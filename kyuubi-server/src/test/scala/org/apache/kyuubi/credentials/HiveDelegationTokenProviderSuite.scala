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

import scala.collection.JavaConverters._

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.kyuubi.WithSecuredHMSContainer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.shaded.hive.metastore.conf.MetastoreConf
import org.apache.kyuubi.shaded.hive.metastore.conf.MetastoreConf.ConfVars
import org.apache.kyuubi.shaded.hive.metastore.security.DelegationTokenIdentifier

class HiveDelegationTokenProviderSuite extends WithSecuredHMSContainer {

  test("obtain hive delegation token") {
    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)

      val kyuubiConf = new KyuubiConf(false)
      val provider = new HiveDelegationTokenProvider
      provider.initialize(hiveConf, kyuubiConf)
      assert(provider.delegationTokensRequired())

      val owner = "who"
      val credentials = new Credentials
      provider.obtainDelegationTokens(owner, credentials)

      val aliasAndToken =
        credentials.getTokenMap.asScala
          .filter(_._2.getKind == DelegationTokenIdentifier.HIVE_DELEGATION_KIND)
          .head
      assert(aliasAndToken._1 == new Text(MetastoreConf.getVar(hiveConf, ConfVars.THRIFT_URIS)))
      assert(aliasAndToken._2 != null)

      val token = aliasAndToken._2
      val tokenIdent = token.decodeIdentifier().asInstanceOf[DelegationTokenIdentifier]
      assertResult(DelegationTokenIdentifier.HIVE_DELEGATION_KIND)(token.getKind)
      assertResult(new Text(owner))(tokenIdent.getOwner)
      val currentUserName = UserGroupInformation.getCurrentUser.getUserName
      assertResult(new Text(currentUserName))(tokenIdent.getRealUser)
    }
  }
}
