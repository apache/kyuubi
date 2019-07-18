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

package yaooqinn.kyuubi.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf}

object KyuubiHiveUtil {

  private val HIVE_PREFIX = "hive."
  private val METASTORE_PREFIX = "metastore."

  val URIS: String = HIVE_PREFIX + METASTORE_PREFIX + "uris"
  val METASTORE_PRINCIPAL: String = HIVE_PREFIX + METASTORE_PREFIX + "kerberos.principal"

  def hiveConf(conf: SparkConf): HiveConf = {
    val hadoopConf = KyuubiSparkUtil.newConfiguration(conf)
    new HiveConf(hadoopConf, classOf[HiveConf])
  }

  def addDelegationTokensToHiveState(ugi: UserGroupInformation): Unit = {
    val state = SessionState.get
    if (state != null) {
      addDelegationTokensToHiveState(state, ugi)
    }
  }

  def addDelegationTokensToHiveState(state: SessionState, ugi: UserGroupInformation): Unit = {
    state.getHdfsEncryptionShim match {
      case shim: org.apache.hadoop.hive.shims.Hadoop23Shims#HdfsEncryptionShim =>
        val hdfsAdmin = ReflectUtils.getFieldValue(shim, "hdfsAdmin")
        val dfs = ReflectUtils.getFieldValue(hdfsAdmin, "dfs")
        dfs match {
          case fs: FileSystem => fs.addDelegationTokens(ugi.getUserName, ugi.getCredentials)
          case _ =>
        }
      case _ =>
    }
  }
}
