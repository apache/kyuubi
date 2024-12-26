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
import java.io.FileInputStream
import java.security.KeyStore
import java.security.cert.X509Certificate

import scala.collection.JavaConverters._

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}

object SSLUtils extends Logging {

  /**
   * Get the keystore certificate latest expiration time.
   */
  private def getKeyStoreExpirationTime(
      keyStorePath: String,
      keyStorePassword: String,
      keyStoreType: Option[String]): Option[Long] = {
    try {
      val keyStore = KeyStore.getInstance(keyStoreType.getOrElse(KeyStore.getDefaultType))
      keyStore.load(new FileInputStream(keyStorePath), keyStorePassword.toCharArray)
      keyStore.aliases().asScala.toSeq.map { alias =>
        keyStore.getCertificate(alias).asInstanceOf[X509Certificate].getNotAfter.getTime
      }.sorted.headOption
    } catch {
      case e: Throwable =>
        error("Error getting keystore expiration time.", e)
        None
    }
  }

  def tracingThriftSSLCertExpiration(
      keyStorePath: Option[String],
      keyStorePassword: Option[String],
      keyStoreType: Option[String]): Unit = {
    if (keyStorePath.isDefined && keyStorePassword.isDefined) {
      SSLUtils.getKeyStoreExpirationTime(
        keyStorePath.get,
        keyStorePassword.get,
        keyStoreType).foreach { expiration =>
        info(s"Thrift SSL Serve KeyStore ${keyStorePath.get} will expire at:" +
          s" ${Utils.getDateFromTimestamp(expiration)}")
        MetricsSystem.tracing { ms =>
          ms.registerGauge(
            MetricsConstants.THRIFT_SSL_CERT_EXPIRATION,
            expiration - System.currentTimeMillis(),
            0L)
        }
      }
    }
  }
}
