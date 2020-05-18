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

package yaooqinn.kyuubi.session.security

import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token
import org.apache.spark.SparkConf

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil
import yaooqinn.kyuubi.utils.KyuubiHiveUtil._

private[security] object HiveTokenCollector extends TokenCollector with Logging {

  override def obtainTokens(conf: SparkConf): Unit = {
    try {
      val c = hiveConf(conf)
      val principal = c.getTrimmed(METASTORE_PRINCIPAL)
      val uris = c.getTrimmed(URIS)
      require(StringUtils.isNotEmpty(principal), METASTORE_PRINCIPAL + " Undefined")
      require(StringUtils.isNotEmpty(uris), URIS + " Undefined")
      val currentUser = UserGroupInformation.getCurrentUser.getUserName
      val credentials = new Credentials()
      KyuubiHadoopUtil.doAsRealUser {
        val hive = Hive.get(c, true)
        info(s"Getting token from Hive Metastore for owner $currentUser via $principal")
        val tokenString = hive.getDelegationToken(currentUser, principal)
        val token = new Token[DelegationTokenIdentifier]
        token.decodeFromUrlString(tokenString)
        info(s"Got " + DelegationTokenIdentifier.stringifyToken(token))
        credentials.addToken(new Text("hive.metastore.delegation.token"), token)
      }
      UserGroupInformation.getCurrentUser.addCredentials(credentials)
    } catch {
      case NonFatal(e) =>
        error("Failed to get token from hive metatore service", e)
    } finally {
      Hive.closeCurrent()
    }
  }

  override def tokensRequired(conf: SparkConf): Boolean = {
    UserGroupInformation.isSecurityEnabled && StringUtils.isNotBlank(hiveConf(conf).get(URIS))
  }
}
