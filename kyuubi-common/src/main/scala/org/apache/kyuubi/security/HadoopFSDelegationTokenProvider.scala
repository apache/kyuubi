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

package org.apache.kyuubi.security

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.security.HadoopFSDelegationTokenProvider.doAsProxyUser

class HadoopFSDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  override val serviceName: String = "hadoopfs"

  override def delegationTokensRequired(hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf,
      owner: String,
      creds: Credentials): Unit =
    doAsProxyUser(owner) {
      val fileSystems =
        HadoopFSDelegationTokenProvider.hadoopFSsToAccess(kyuubiConf, hadoopConf)
      fileSystems.foreach { fs =>
        info(s"getting token for: $fs")
        fs.addDelegationTokens(null, creds)
      }
    }

}

private[security] object HadoopFSDelegationTokenProvider {

  val proxyUserCache = new ConcurrentHashMap[String, UserGroupInformation]()

  def hadoopFSsToAccess(kyuubiConf: KyuubiConf, hadoopConf: Configuration): Set[FileSystem] = {
    val defaultFS = FileSystem.get(hadoopConf)
    val filesystemsToAccess = kyuubiConf
      .get(KyuubiConf.CREDENTIALS_HADOOP_FS_URLS)
      .map(new Path(_).getFileSystem(hadoopConf))
      .toSet

    filesystemsToAccess + defaultFS
  }

  def doAsProxyUser[T](proxyUser: String)(f: => T): T = {
    val ugi = proxyUserCache.computeIfAbsent(
      proxyUser,
      proxyUser =>
        UserGroupInformation
          .createProxyUser(proxyUser, UserGroupInformation.getCurrentUser))

    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      ugi.doAs(new PrivilegedExceptionAction[T] {
        override def run(): T = f
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause).getOrElse(e)
    }
  }

}
