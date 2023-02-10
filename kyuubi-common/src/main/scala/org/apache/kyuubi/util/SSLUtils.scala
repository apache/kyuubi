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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_SSL_KEYSTORE_TYPE

object SSLUtils extends Logging {
  /**
   * Get the keystore certificate latest expiration time.
   */
  def getKeyStoreExpirationTime(conf: KyuubiConf): Option[Long] = {
    val keyStorePath = conf.get(KyuubiConf.FRONTEND_SSL_KEYSTORE_PATH)
    val keyStorePassword = conf.get(KyuubiConf.FRONTEND_SSL_KEYSTORE_PASSWORD)
    val keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE).getOrElse(KeyStore.getDefaultType)

    try {
      keyStorePath.zip(keyStorePassword) match {
        case Seq((path, password)) =>
          val keyStore = KeyStore.getInstance(keyStoreType)
          keyStore.load(new FileInputStream(path), password.toCharArray)
          keyStore.aliases().asScala.toSeq.map { alias =>
            keyStore.getCertificate(alias).asInstanceOf[X509Certificate].getNotAfter.getTime
          }.sorted.headOption
        case _ => None
      }
    } catch {
      case e: Throwable =>
        error("Error getting keystore expiration time.", e)
        None
    }
  }
}
