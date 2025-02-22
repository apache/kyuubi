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
import java.net.URI
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.credentials.HadoopFsDelegationTokenProvider.{doAsProxyUser, validatedFsUris}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class HadoopFsDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  private var tokenRequired: Boolean = _
  private var hadoopConf: Configuration = _
  private var kyuubiConf: KyuubiConf = _
  private var fsUris: Seq[URI] = _

  override val serviceName: String = "hadoopfs"

  override def initialize(hadoopConf: Configuration, kyuubiConf: KyuubiConf): Unit = {
    this.tokenRequired =
      SecurityUtil.getAuthenticationMethod(hadoopConf) != AuthenticationMethod.SIMPLE

    this.kyuubiConf = kyuubiConf
    this.fsUris = validatedFsUris(kyuubiConf, hadoopConf)
    this.hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf, loadDefaults = false)

    // Using HdfsConfiguration to ensure hdfs-site.xml is loaded
    new HdfsConfiguration(hadoopConf).iterator().asScala.foreach(e =>
      this.hadoopConf.set(e.getKey, e.getValue))
    // Disable FileSystem cache as its size grows at each invocation of #obtainDelegationTokens
    this.fsUris.foreach(uri =>
      this.hadoopConf.setBoolean(s"fs.${uri.getScheme}.impl.disable.cache", true))
  }

  override def delegationTokensRequired(): Boolean = tokenRequired

  override def obtainDelegationTokens(owner: String, creds: Credentials): Unit = {
    doAsProxyUser(owner) {
      val fileSystems = fsUris.map { uri =>
        FileSystem.get(uri, hadoopConf) -> uri
      }.toMap

      try {
        fileSystems.foreach { case (fs, uri) =>
          info(s"getting token owned by $owner for: $uri")
          try {
            fs.addDelegationTokens("", creds)
          } catch {
            case e: Exception =>
              throw new KyuubiException(s"Failed to get token owned by $owner for: $uri", e)
          }
        }
      } finally {
        // Token renewal interval is longer than FileSystems' underlying connections' max idle time.
        // Close FileSystems won't lose efficiency.
        fileSystems.keys.foreach(_.close())
      }
    }
  }

}

object HadoopFsDelegationTokenProvider extends Logging {

  def validatedFsUris(kyuubiConf: KyuubiConf, hadoopConf: Configuration): Seq[URI] = {
    val uris = kyuubiConf.get(KyuubiConf.CREDENTIALS_HADOOP_FS_URIS) :+
      hadoopConf.get("fs.defaultFS", "file:///")
    uris.flatMap { str =>
      try {
        val uri = URI.create(str)
        FileSystem.get(uri, hadoopConf)
        Some(uri)
      } catch {
        case e: Throwable =>
          warn(s"Failed to get Hadoop FileSystem instance by URI: $str", e)
          None
      }
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
