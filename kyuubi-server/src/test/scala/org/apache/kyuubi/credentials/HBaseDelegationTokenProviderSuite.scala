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

import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.kyuubi.WithSecuredHBaseContainer
import org.apache.kyuubi.config.KyuubiConf

class HBaseDelegationTokenProviderSuite extends WithSecuredHBaseContainer {

  test("obtain hbase delegation token") {
    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)

      val kyuubiConf = new KyuubiConf(false)
      val provider = new HBaseDelegationTokenProvider()
      provider.initialize(hbaseConf, kyuubiConf)
      assert(provider.delegationTokensRequired())

      val owner = "who"
      val credentials = new Credentials
      provider.obtainDelegationTokens(owner, credentials)

      val aliasAndToken =
        credentials.getTokenMap.asScala
          .head
      assert(aliasAndToken._2 != null)

      val token = aliasAndToken._2
      val tokenIdent = token.decodeIdentifier().asInstanceOf[AuthenticationTokenIdentifier]
      assertResult(new Text("HBASE_AUTH_TOKEN"))(token.getKind)
      assertResult(owner)(tokenIdent.getUsername)
    }
  }
}
