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
import java.util.{Map => JMap}
import javax.security.auth.Subject

import scala.collection.JavaConverters._

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.kyuubi.config.KyuubiConf

object KyuubiHadoopUtils {

  private val subjectField =
    classOf[UserGroupInformation].getDeclaredField("subject")
  subjectField.setAccessible(true)

  private val getCredentialsInternalMethod =
    classOf[UserGroupInformation].getDeclaredMethod(
      "getCredentialsInternal",
      Array.empty[Class[_]]: _*)
  getCredentialsInternalMethod.setAccessible(true)

  private val tokenMapField =
    classOf[Credentials].getDeclaredField("tokenMap")
  tokenMapField.setAccessible(true)

  def newHadoopConf(conf: KyuubiConf): Configuration = {
    val hadoopConf = new Configuration()
    conf.getAll.foreach { case (k, v) => hadoopConf.set(k, v) }
    hadoopConf
  }

  def getServerPrincipal(principal: String): String = {
    SecurityUtil.getServerPrincipal(principal, "0.0.0.0")
  }

  def encodeCredentials(creds: Credentials): String = {
    val byteStream = new ByteArrayOutputStream
    creds.writeTokenStorageToStream(new DataOutputStream(byteStream))

    val encoder = new Base64(0, null, false)
    encoder.encodeToString(byteStream.toByteArray)
  }

  def decodeCredentials(newValue: String): Credentials = {
    val decoder = new Base64(0, null, false)
    val decoded = decoder.decode(newValue)

    val byteStream = new ByteArrayInputStream(decoded)
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(byteStream))
    creds
  }

  /**
   * Get all tokens in [[UserGroupInformation#subject]] including
   * [[org.apache.hadoop.security.token.Token.PrivateToken]] as
   * [[UserGroupInformation#getCredentials]] returned Credentials do not contain
   * [[org.apache.hadoop.security.token.Token.PrivateToken]].
   */
  def getCredentialsInternal(ugi: UserGroupInformation): Credentials = {
    // Synchronize to avoid credentials being written while cloning credentials
    subjectField.get(ugi).asInstanceOf[Subject] synchronized {
      new Credentials(getCredentialsInternalMethod.invoke(ugi).asInstanceOf[Credentials])
    }
  }

  /**
   * Get [[Credentials#tokenMap]] by reflection as [[Credentials#getTokenMap]] is not present before
   * Hadoop 3.2.1.
   */
  def getTokenMap(credentials: Credentials): Map[Text, Token[_ <: TokenIdentifier]] = {
    tokenMapField.get(credentials)
      .asInstanceOf[JMap[Text, Token[_ <: TokenIdentifier]]]
      .asScala
      .toMap
  }

  def getTokenIssueDate(token: Token[_ <: TokenIdentifier]): Long = {
    // It is safe to deserialize any token identifier to hdfs `DelegationTokenIdentifier`
    // as all token identifiers have the same binary format.
    val tokenIdentifier = new DelegationTokenIdentifier
    val buf = new ByteArrayInputStream(token.getIdentifier)
    val in = new DataInputStream(buf)
    tokenIdentifier.readFields(in)
    tokenIdentifier.getIssueDate
  }
}
