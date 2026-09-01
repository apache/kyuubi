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

package org.apache.kyuubi.server.flight

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.KeyStore
import java.util.Base64

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

/**
 * Resolves PEM material for Arrow Flight TLS. Prefers explicit PEM files and
 * can materialize temporary PEM files from the shared Java keystore settings.
 */
object KyuubiFlightTlsUtils extends Logging {

  case class TlsMaterial(certFile: File, keyFile: File, temporary: Boolean) {
    def cleanup(): Unit = if (temporary) {
      Seq(certFile, keyFile).foreach { file =>
        try Files.deleteIfExists(file.toPath)
        catch {
          case NonFatal(e) => warn(s"Failed to delete temporary Flight TLS file $file", e)
        }
      }
    }
  }

  def resolve(conf: KyuubiConf): TlsMaterial = {
    val cert = conf.get(FRONTEND_FLIGHT_SQL_SSL_CERT_FILE)
    val key = conf.get(FRONTEND_FLIGHT_SQL_SSL_KEY_FILE)
    if (cert.isDefined || key.isDefined) {
      if (cert.isEmpty || key.isEmpty) {
        throw new IllegalArgumentException(
          s"Both ${FRONTEND_FLIGHT_SQL_SSL_CERT_FILE.key} and " +
            s"${FRONTEND_FLIGHT_SQL_SSL_KEY_FILE.key} must be set for Flight SQL TLS")
      }
      val certFile = new File(cert.get)
      val keyFile = new File(key.get)
      if (!certFile.isFile) {
        throw new IllegalArgumentException(s"Flight SQL TLS certificate not found: ${cert.get}")
      }
      if (!keyFile.isFile) {
        throw new IllegalArgumentException(s"Flight SQL TLS private key not found: ${key.get}")
      }
      TlsMaterial(certFile, keyFile, temporary = false)
    } else {
      materializeFromKeystore(conf)
    }
  }

  private def materializeFromKeystore(conf: KyuubiConf): TlsMaterial = {
    val keyStorePath = conf.get(FRONTEND_SSL_KEYSTORE_PATH).getOrElse {
      throw new IllegalArgumentException(
        s"${FRONTEND_FLIGHT_SQL_SSL_CERT_FILE.key}/${FRONTEND_FLIGHT_SQL_SSL_KEY_FILE.key} or " +
          s"${FRONTEND_SSL_KEYSTORE_PATH.key} must be configured when Flight SQL TLS is enabled")
    }
    val keyStorePassword = conf.get(FRONTEND_SSL_KEYSTORE_PASSWORD).getOrElse {
      throw new IllegalArgumentException(
        s"${FRONTEND_SSL_KEYSTORE_PASSWORD.key} must be configured " +
          "for Flight SQL TLS keystore fallback")
    }
    val keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE).getOrElse(KeyStore.getDefaultType)
    val keyStore = KeyStore.getInstance(keyStoreType)
    val input = Files.newInputStream(new File(keyStorePath).toPath)
    try {
      keyStore.load(input, keyStorePassword.toCharArray)
    } finally {
      input.close()
    }

    val aliases = keyStore.aliases().asScala.toSeq
    val alias = aliases.find(a => keyStore.isKeyEntry(a)).getOrElse {
      throw new IllegalArgumentException(
        s"No private key entry found in keystore $keyStorePath for Flight SQL TLS")
    }
    val key = keyStore.getKey(alias, keyStorePassword.toCharArray)
    val chain = keyStore.getCertificateChain(alias)
    if (key == null || chain == null || chain.isEmpty) {
      throw new IllegalArgumentException(
        s"Keystore entry $alias does not contain a usable certificate chain/private key")
    }

    val certFile = Files.createTempFile("kyuubi-flight-cert-", ".pem").toFile
    val keyFile = Files.createTempFile("kyuubi-flight-key-", ".pem").toFile
    certFile.deleteOnExit()
    keyFile.deleteOnExit()
    writePem(certFile, "CERTIFICATE", chain.map(_.getEncoded))
    writePem(keyFile, "PRIVATE KEY", Array(key.getEncoded))
    // Best-effort restrictive permissions on POSIX systems.
    try {
      certFile.setReadable(false, false)
      certFile.setReadable(true, true)
      keyFile.setReadable(false, false)
      keyFile.setReadable(true, true)
      keyFile.setWritable(false, false)
      keyFile.setWritable(true, true)
    } catch {
      case NonFatal(_) => // ignore on non-POSIX filesystems
    }
    TlsMaterial(certFile, keyFile, temporary = true)
  }

  private def writePem(file: File, label: String, derBlocks: Array[Array[Byte]]): Unit = {
    val writer = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.US_ASCII)
    try {
      derBlocks.foreach { der =>
        writer.write(s"-----BEGIN $label-----\n")
        val encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
          .encodeToString(der)
        writer.write(encoded)
        writer.write(s"\n-----END $label-----\n")
      }
    } finally {
      writer.close()
    }
  }

  def validateCertPresent(material: TlsMaterial): Unit = {
    val bytes = Files.readAllBytes(material.certFile.toPath)
    if (bytes.isEmpty) {
      throw new IllegalArgumentException("Flight SQL TLS certificate file is empty")
    }
  }
}
