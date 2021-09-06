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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.credentials.HadoopFsDelegationTokenProvider.{disableFsCache, doAsProxyUser}

class HadoopFsDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  override val serviceName: String = "hadoopfs"

  override def delegationTokensRequired(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf): Boolean = {
    SecurityUtil.getAuthenticationMethod(hadoopConf) != AuthenticationMethod.SIMPLE
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf,
      owner: String,
      creds: Credentials): Unit = {
    // FileSystem objects are cached in FileSystem.CACHE by a composite key.
    // The UserGroupInformation object used to create it is part of that key.
    // If cache is enabled, new FileSystem objects are created and cached at every method
    // invocation.
    val internalConf = disableFsCache(kyuubiConf, hadoopConf)

    doAsProxyUser(owner) {
      val fileSystems =
        HadoopFsDelegationTokenProvider.hadoopFSsToAccess(kyuubiConf, internalConf)

      try {
        fileSystems.foreach { fs =>
          info(s"getting token owned by $owner for: $fs")
          fs.addDelegationTokens(null, creds)
        }
      } finally {
        // Token renewal interval is longer than FileSystems' underlying connections' max idle time.
        // Close FileSystems won't lose efficiency.
        fileSystems.foreach(_.close())
      }
    }
  }

}

object HadoopFsDelegationTokenProvider {

  def disableFsCache(kyuubiConf: KyuubiConf, hadoopConf: Configuration): Configuration = {
    val newConf = new Configuration(false)
    hadoopConf.iterator().asScala.foreach(e => newConf.set(e.getKey, e.getValue))

    hadoopFSsToAccess(kyuubiConf, hadoopConf)
      .foreach(fs => newConf.setBoolean(s"fs.${fs.getScheme}.impl.disable.cache", true))
    newConf
  }

  def hadoopFSsToAccess(kyuubiConf: KyuubiConf, hadoopConf: Configuration): Set[FileSystem] = {
    val defaultFS = FileSystem.get(hadoopConf)
    val filesystemsToAccess = kyuubiConf
      .get(KyuubiConf.CREDENTIALS_HADOOP_FS_URLS)
      .map(new Path(_).getFileSystem(hadoopConf))
      .toSet

    filesystemsToAccess + defaultFS
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
