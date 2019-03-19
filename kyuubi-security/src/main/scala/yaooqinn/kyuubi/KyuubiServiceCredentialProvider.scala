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

/**
 * An implementation of [[ServiceCredentialProvider]] of Apache Spark, this class will be called
 * in both Kyuubi Server and the ApplicationMasters. Main purpose here is updating the
 * ApplicationMaster's delegation tokens and does not care whether the tokens to be written to HDFS
 * for other executors to do their token update job or not. The reason why we do such thing here is
 * that this part of Spark source code is totally a mess and changed capriciously before the Spark
 * 3.x which is not release yet. So if tokens are written to HDFS, the driver(here as same as Kyuubi
 * Server instance) and executor can update their token purposely. But if they are failed, with some
 * warning messages ApplicationMaster will schedule this task in every hour which would be helpful
 * enough to invoke this class which help us to renew the ApplicationMaster 's delegation tokens.
 * Another un-harmful problem the driver and executors will continuously run token update task in
 * every minute, which is always annoyingly printing 'CredentialUpdater: Scheduling credentials
 * refresh from HDFS in 60000 ms' to logs. Kyuubi will post all delegation tokens to executor side
 * by the server itself but not to the ApplicationMaster, so the ApplicationMaster must update
 * tokens by itself in case of requesting new executors in runtime.
 *
 *
 * In Kyuubi Server:
 *    This is called when SparkContext instantiating itself, with `spark.yarn.credentials.file`,
 *    `spark.yarn.credentials.renewalTime`, `spark.yarn.credentials.updateTime` set. Because
 *    inside Kyuubi we explicitly turn off Other credential providers of Spark, so the renew time
 *    and the update time is decided by [[KyuubiServiceCredentialProvider#obtainCredentials]], a
 *    credentials file will be set to `spark.yarn.credentials.file` but not be created yet. If the
 *    Spark Driver(here as same as Kyuubi Server instance) itself and the executors detect
 *    `spark.yarn.credentials.file` is set, a credential updater thead will start and periodically
 *    get the latest `spark.yarn.credentials.file` to update their own tokens if the file is
 *    present.
 * In ApplicationMaster:
 *    If the ApplicationMaster detects the `spark.yarn.credentials.file` is set, it will launch a
 *    token renew thread to call all token provider's obtainCredentials method, so the class will be
 *    called to update its own delegation token. So when new containers are requested by this
 *    ApplicationMaster, it will add right tokens to them. Kyuubi Server side will generate tokens
 *    and post the via UpdateToken messages to executors, so unlike it does in a normal Spark
 *    application, here the ApplicationMaster only needs to take care of itself.
 */
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
    try {
      val tokenRenewer = renewer(hadoopConf)
      hadoopFStoAccess(sparkConf, hadoopConf).foreach { fs =>
        fs.addDelegationTokens(tokenRenewer, creds)
      }
      UserGroupInformation.getCurrentUser.addCredentials(creds)
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to renew token", e)
    }
    val interval = sparkConf.getTimeAsMs("spark.kyuubi.backend.session.token.renew.interval", "2h")
    info(s"Token updated, next renew interval will be ${interval / 1000} seconds")
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
