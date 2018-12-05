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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.KyuubiSparkUtil._
import org.apache.spark.SparkConf

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.service.ServiceException

/**
 * Token collector for secured HDFS FileSystems
 */
private[security] object HDFSTokenCollector extends TokenCollector with Logging {

  private def hadoopFStoAccess(conf: SparkConf, hadoopConf: Configuration): Set[FileSystem] = {
    val fileSystems = conf.getOption(ACCESS_FSS)
      .orElse(conf.getOption(ACCESS_NNS)) match {
      case Some(nns) => nns.split(",").map(new Path(_).getFileSystem(hadoopConf)).toSet
      case _ => Set.empty[FileSystem]
    }

    fileSystems +
      conf.getOption(STAGING_DIR).map(new Path(_).getFileSystem(hadoopConf))
        .getOrElse(FileSystem.get(hadoopConf))
  }

  private def renewer(hadoopConf: Configuration): String = {
    val tokenRenewer = Master.getMasterPrincipal(hadoopConf)
    debug("Delegation token renewer is: " + tokenRenewer)

    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer."
      error(errorMessage)
      throw new ServiceException(errorMessage)
    }
    tokenRenewer
  }

  override def obtainTokens(conf: SparkConf): Unit = {
    val hadoopConf = newConfiguration(conf)
    val tokenRenewer = renewer(hadoopConf)
    val creds = new Credentials()
    hadoopFStoAccess(conf, hadoopConf).foreach { fs =>
      fs.addDelegationTokens(tokenRenewer, creds)
    }
    UserGroupInformation.getCurrentUser.addCredentials(creds)
  }
}
