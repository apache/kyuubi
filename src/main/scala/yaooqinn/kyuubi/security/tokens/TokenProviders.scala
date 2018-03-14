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

package yaooqinn.kyuubi.security.tokens

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.{SparkConf, SparkUtils}

import yaooqinn.kyuubi.Logging

/**
 * A helper class for Backend Service to obtain delegation tokens from hive metastore and hadoop
 * file systems for the impersonating user.
 */
private[kyuubi] object TokenProviders extends Logging {
  /** obtain delegation token for the give user from hive metastore */
  private[this] def getDelegationTokenFromMetaStore(
      owner: String,
      conf: SparkConf): Set[Token[_ <: TokenIdentifier]] = synchronized {
    try {
      val tokenStr = SparkUtils.doAsRealUser {
        Hive.get(SparkUtils.newConfiguration(conf), classOf[HiveConf])
          .getDelegationToken(owner, owner)
      }
      info("getting token from service hive " + tokenStr)
      val token = new Token[DelegationTokenIdentifier]()
      token.decodeFromUrlString(tokenStr)
    Set(token)
  } catch {
      case e: Exception =>
        error(s"Failed to get token from service hive metastore", e)
        Set.empty
    } finally {
      SparkUtils.tryLogNonFatalError(Hive.closeCurrent())
    }
  }

  /** obtain delegation token for the give user from hadoop filesystems */
  private[this] def getDelegationTokenFromHadoopFileSystem(
      owner: String,
      conf: SparkConf): Set[Token[_ <: TokenIdentifier]] = {
    try {
      info("getting token from service hadoop file systems")
      hadoopFSsToAccess(conf).map { fs =>
        info("getting token for namenode: " + fs)
        fs.getDelegationToken(owner)
      }
    } catch {
      case e: Exception =>
        error(s"Failed to get token from service hadoop file systems", e)
        Set.empty
    }
  }

  /** The filesystems for which YARN should fetch delegation tokens. */
  private[this] def hadoopFSsToAccess(
      sparkConf: SparkConf): Set[FileSystem] = {
    val hadoopConf = SparkUtils.newConfiguration(sparkConf)
    val filesystemsToAccess = sparkConf.getOption(SparkUtils.NAMENODES_TO_ACCESS)
      .map(new Path(_).getFileSystem(hadoopConf))
      .toSet
    filesystemsToAccess
  }

  def obtainDelegationTokens(owner: String, conf: SparkConf): Set[Token[_ <: TokenIdentifier]] = {
    getDelegationTokenFromMetaStore(owner, conf) ++
      getDelegationTokenFromHadoopFileSystem(owner, conf)
  }
}
