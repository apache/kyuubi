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
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.token.ClientTokenUtil
import org.apache.hadoop.security.Credentials

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.credentials.HadoopFsDelegationTokenProvider.doAsProxyUser

private class HBaseDelegationTokenProvider
  extends HadoopDelegationTokenProvider with Logging {

  override def serviceName: String = "hbase"
  private var tokenRequired: Boolean = _
  private var hbaseConf: Configuration = _
  private var kyuubiConf: KyuubiConf = _

  override def initialize(hadoopConf: Configuration, kyuubiConf: KyuubiConf): Unit = {
    this.kyuubiConf = kyuubiConf
    this.hbaseConf = hadoopConf
    this.tokenRequired =
      hbaseConf.get("hbase.security.authentication", "simple").toLowerCase() == "kerberos"
  }

  override def obtainDelegationTokens(
      owner: String,
      creds: Credentials): Unit = {
    doAsProxyUser(owner) {
      var conn: Connection = null
      try {
        info(s"Getting HBase delegation token for ${owner} ...")
        conn = ConnectionFactory.createConnection(hbaseConf)
        val token = ClientTokenUtil.obtainToken(conn)
        info(s"Get HBase delegation token ${token}")
        creds.addToken(token.getService, token)
      } catch {
        case e: Throwable =>
          throw new KyuubiException(s"Failed to get HBase delegation token owned by $owner", e)
      } finally {
        if (conn != null) {
          conn.close()
        }
      }
    }
  }

  override def delegationTokensRequired(): Boolean = {
    tokenRequired
  }
}
