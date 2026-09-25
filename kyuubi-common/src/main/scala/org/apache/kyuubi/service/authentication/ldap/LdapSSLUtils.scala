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

package org.apache.kyuubi.service.authentication.ldap

import java.io.{FileInputStream, IOException}
import java.security.{GeneralSecurityException, KeyStore}
import java.security.cert.{CertificateFactory, X509Certificate}
import javax.net.ssl.{SSLContext, TrustManagerFactory}

import scala.collection.JavaConverters._

private[ldap] object LdapSSLUtils {

  @throws[GeneralSecurityException]
  @throws[IOException]
  def createSSLContext(
      trustStorePath: String,
      trustStorePassword: String,
      trustStoreType: String): SSLContext = {
    val trustStore =
      if (Option(trustStorePath).exists(_.trim.nonEmpty)) {
        loadTrustStore(trustStorePath, trustStorePassword, trustStoreType)
      } else {
        null
      }

    val trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(trustStore)

    val sslContext = SSLContext.getInstance("SSL")
    sslContext.init(null, trustManagerFactory.getTrustManagers, null)
    sslContext
  }

  @throws[GeneralSecurityException]
  @throws[IOException]
  private def loadTrustStore(
      trustStorePath: String,
      trustStorePassword: String,
      trustStoreType: String): KeyStore = {
    val certificatesKeyStore =
      try {
        loadCertificates(trustStorePath)
      } catch {
        case _: GeneralSecurityException | _: IOException => None
      }
    certificatesKeyStore
      .getOrElse(loadKeyStore(trustStorePath, trustStorePassword, trustStoreType))
  }

  @throws[GeneralSecurityException]
  @throws[IOException]
  private def loadKeyStore(
      trustStorePath: String,
      trustStorePassword: String,
      trustStoreType: String): KeyStore = {
    val trustStore =
      KeyStore.getInstance(
        Option(trustStoreType)
          .filter(_.trim.nonEmpty)
          .getOrElse(KeyStore.getDefaultType))
    val in = new FileInputStream(trustStorePath)
    try {
      trustStore.load(in, toCharArray(trustStorePassword))
    } finally {
      in.close()
    }
    trustStore
  }

  @throws[GeneralSecurityException]
  @throws[IOException]
  private def loadCertificates(trustStorePath: String): Option[KeyStore] = {
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val certificateChain = {
      val in = new FileInputStream(trustStorePath)
      try {
        val certificates = certificateFactory.generateCertificates(in)
        certificates.asScala.map(_.asInstanceOf[X509Certificate]).toSeq
      } finally {
        in.close()
      }
    }

    if (certificateChain.isEmpty) {
      None
    } else {
      val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
      trustStore.load(null, null)
      var index = 1
      certificateChain.foreach { certificate =>
        val certificateAlias = s"Certificate_$index";
        trustStore.setCertificateEntry(certificateAlias, certificate)
        index += 1
      }
      Some(trustStore)
    }
  }

  private def toCharArray(password: String): Array[Char] = {
    if (password == null) null else password.toCharArray
  }
}
