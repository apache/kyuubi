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

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.credentials.HadoopFsDelegationTokenProvider.{disableFsCache, doAsProxyUser}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class HadoopFsDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  private var tokenRequired: Boolean = _
  private var hadoopConf: Configuration = _
  private var kyuubiConf: KyuubiConf = _

  override val serviceName: String = "hadoopfs"

  override def initialize(hadoopConf: Configuration, kyuubiConf: KyuubiConf): Unit = {
    this.tokenRequired =
      SecurityUtil.getAuthenticationMethod(hadoopConf) != AuthenticationMethod.SIMPLE

    // FileSystem objects are cached in FileSystem.CACHE by a composite key.
    // The UserGroupInformation object used to create it is part of that key.
    // If cache is enabled, new FileSystem objects are created and cached at every method
    // invocation.
    this.hadoopConf = disableFsCache(kyuubiConf, new HdfsConfiguration(hadoopConf))
    this.kyuubiConf = kyuubiConf
  }

  override def delegationTokensRequired(): Boolean = tokenRequired

  override def obtainDelegationTokens(owner: String, creds: Credentials): Unit = {
    doAsProxyUser(owner) {
      val fileSystems =
        HadoopFsDelegationTokenProvider.hadoopFSsToAccess(kyuubiConf, hadoopConf)

      try {
        // Renewer is not needed. But setting a renewer can avoid potential NPE.
        val renewer = UserGroupInformation.getCurrentUser.getUserName
        fileSystems.foreach { fs =>
          info(s"getting token owned by $owner for: $fs")
          fs.addDelegationTokens(renewer, creds)
        }
      } finally {
        // Token renewal interval is longer than FileSystems' underlying connections' max idle time.
        // Close FileSystems won't lose efficiency.
        fileSystems.foreach(_.close())
      }
    }
  }

}

object HadoopFsDelegationTokenProvider extends Logging {

  def disableFsCache(kyuubiConf: KyuubiConf, hadoopConf: Configuration): Configuration = {
    // Avoid unnecessary disk io by not loading default resources
    val newConf = KyuubiHadoopUtils.newHadoopConf(
      kyuubiConf,
      loadDefaults = false)

    hadoopConf.iterator().asScala.foreach(e => newConf.set(e.getKey, e.getValue))

    hadoopFSsToAccess(kyuubiConf, hadoopConf)
      .foreach(fs => newConf.setBoolean(s"fs.${fs.getScheme}.impl.disable.cache", true))
    newConf
  }

  def hadoopFSsToAccess(kyuubiConf: KyuubiConf, hadoopConf: Configuration): Set[FileSystem] = {
    val filesystemsToAccess = kyuubiConf
      .get(KyuubiConf.CREDENTIALS_HADOOP_FS_URIS)
      .flatMap { uri =>
        Try(new Path(uri).getFileSystem(hadoopConf)) match {
          case Success(value) =>
            Some(value)
          case Failure(e) =>
            warn(s"Failed to get Hadoop FileSystem instance by URI: $uri", e)
            None
        }
      }
      .toSet

    Try(FileSystem.get(hadoopConf)) match {
      case Success(value) =>
        filesystemsToAccess + value
      case Failure(e) =>
        warn(s"Failed to get default Hadoop FileSystem instance", e)
        filesystemsToAccess
    }
  }

  def doAsProxyUser[T](proxyUser: String)(f: => T): T = {
    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      val ugi = UserGroupInformation.createProxyUser(proxyUser, UserGroupInformation.getCurrentUser)
      ugi.doAs(new PrivilegedExceptionAction[T] {
        override def run(): T = f
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause).getOrElse(e)
    }
  }

}
