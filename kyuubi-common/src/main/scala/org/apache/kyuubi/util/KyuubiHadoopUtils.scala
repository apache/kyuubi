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

package org.apache.kyuubi.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Base64, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX
import org.apache.kyuubi.util.reflect.ReflectUtils._

object KyuubiHadoopUtils extends Logging {

  def newHadoopConf(
      conf: KyuubiConf,
      loadDefaults: Boolean = true): Configuration = {
    val hadoopConf = new Configuration(loadDefaults)
    List("HADOOP_CONF_DIR", "YARN_CONF_DIR").foreach { envKey =>
      conf.getAllWithPrefix(KYUUBI_ENGINE_ENV_PREFIX, "").get(envKey).foreach { dir =>
        // 处理异常文件夹不存在等等
        try {
          new java.io.File(dir).listFiles.filter(_.getName.endsWith(".xml"))
            .foreach { file =>
              hadoopConf.addResource(file.toURI.toURL)
            }
        } catch {
          case e: Exception =>
            warn(s"Failed to load $envKey from $dir", e)
        }
      }
    }
    conf.getAll
      .foreach { case (k, v) => hadoopConf.set(k, v) }
    hadoopConf
  }

  def newYarnConfiguration(conf: KyuubiConf): YarnConfiguration = {
    new YarnConfiguration(newHadoopConf(conf))
  }

  def getServerPrincipal(principal: String): String = {
    SecurityUtil.getServerPrincipal(principal, "0.0.0.0")
  }

  def encodeCredentials(creds: Credentials): String = {
    val byteStream = new ByteArrayOutputStream
    creds.writeTokenStorageToStream(new DataOutputStream(byteStream))

    Base64.getMimeEncoder.encodeToString(byteStream.toByteArray)
  }

  def decodeCredentials(newValue: String): Credentials = {
    val decoded = Base64.getMimeDecoder.decode(newValue)

    val byteStream = new ByteArrayInputStream(decoded)
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(byteStream))
    creds
  }

  def serializeCredentials(creds: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    byteStream.toByteArray
  }

  def deserializeCredentials(tokenBytes: Array[Byte]): Credentials = {
    val tokensBuf = new ByteArrayInputStream(tokenBytes)

    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(tokensBuf))
    creds
  }

  /**
   * Get [[Credentials#tokenMap]] by reflection as [[Credentials#getTokenMap]] is not present before
   * Hadoop 3.2.1.
   */
  def getTokenMap(credentials: Credentials): Map[Text, Token[_ <: TokenIdentifier]] =
    getField[JMap[Text, Token[_ <: TokenIdentifier]]](credentials, "tokenMap").asScala.toMap

  def getTokenIssueDate(token: Token[_ <: TokenIdentifier]): Option[Long] = {
    token.decodeIdentifier() match {
      case tokenIdent: AbstractDelegationTokenIdentifier =>
        Some(tokenIdent.getIssueDate)
      case null =>
        // TokenIdentifiers not found in ServiceLoader
        val tokenIdentifier = new DelegationTokenIdentifier
        val buf = new ByteArrayInputStream(token.getIdentifier)
        val in = new DataInputStream(buf)
        Try(tokenIdentifier.readFields(in)) match {
          case Success(_) =>
            Some(tokenIdentifier.getIssueDate)
          case Failure(e) =>
            warn(s"Can not decode identifier of token $token", e)
            None
        }
      case tokenIdent =>
        debug(s"Unsupported TokenIdentifier kind: ${tokenIdent.getKind}")
        None
    }
  }

  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
   */
  def addPathToEnvironment(env: HashMap[String, String], key: String, value: String): Unit = {
    val newValue =
      if (env.contains(key)) {
        env(key) + ApplicationConstants.CLASS_PATH_SEPARATOR + value
      } else {
        value
      }
    env.put(key, newValue)
  }
}
