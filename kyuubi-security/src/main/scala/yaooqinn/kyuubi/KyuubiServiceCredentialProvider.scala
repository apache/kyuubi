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

package yaooqinn.kyuubi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider

class KyuubiServiceCredentialProvider extends ServiceCredentialProvider with Logging {

  /**
   * Name of the service to provide credentials. This name should unique, Spark internally will
   * use this name to differentiate credential provider.
   */
  override def serviceName: String = "kyuubi"

  /**
   * Obtain credentials for this service and get the time of the next renewal.
   * @param hadoopConf Configuration of current Hadoop Compatible system.
   * @param sparkConf Spark configuration.
   * @param creds Credentials to add tokens and security keys to.
   * @return If this Credential is renewable and can be renewed, return the time of the next
   *         renewal, otherwise None should be returned.
   */
  override def obtainCredentials(hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    val tokenRenewer = renewer(hadoopConf)
    hadoopFStoAccess(sparkConf, hadoopConf).foreach { fs =>
      fs.addDelegationTokens(tokenRenewer, creds)
    }
    UserGroupInformation.getCurrentUser.addCredentials(creds)
    val interval = sparkConf.getTimeAsMs("spark.kyuubi.credential.renew.interval", "2h")
    info(s"Token updated, next renew interval will be $interval")
    Some(System.currentTimeMillis() + interval)
  }

  private def renewer(hadoopConf: Configuration): String = {
    val tokenRenewer = Master.getMasterPrincipal(hadoopConf)
    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer."
      throw new RuntimeException(errorMessage)
    }
    tokenRenewer
  }

  private def hadoopFStoAccess(conf: SparkConf, hadoopConf: Configuration): Set[FileSystem] = {
    val fileSystems = conf.getOption("spark.yarn.access.hadoopFileSystems")
      .orElse(conf.getOption("spark.yarn.access.namenodes")) match {
      case Some(nns) => nns.split(",").map(new Path(_).getFileSystem(hadoopConf)).toSet
      case _ => Set.empty[FileSystem]
    }

    fileSystems +
      conf.getOption("spark.yarn.stagingDir").map(new Path(_).getFileSystem(hadoopConf))
        .getOrElse(FileSystem.get(hadoopConf))
  }
}
