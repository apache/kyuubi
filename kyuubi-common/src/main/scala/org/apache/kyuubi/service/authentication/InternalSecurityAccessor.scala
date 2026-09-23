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

package org.apache.kyuubi.service.authentication

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import org.apache.hadoop.classification.VisibleForTesting

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiConf.InternalSecurityCryptoCompatibilityMode

class InternalSecurityAccessor(conf: KyuubiConf, val isServer: Boolean) {
  val cryptoKeyLengthBytes = conf.get(INTERNAL_SECURITY_CRYPTO_KEY_LENGTH) / java.lang.Byte.SIZE
  val cryptoIvLength = conf.get(INTERNAL_SECURITY_CRYPTO_IV_LENGTH)
  val cryptoKeyAlgorithm = conf.get(INTERNAL_SECURITY_CRYPTO_KEY_ALGORITHM)
  val cryptoCipher = conf.get(INTERNAL_SECURITY_CRYPTO_CIPHER_TRANSFORMATION)

  private val tokenMaxLifeTime: Long = conf.get(INTERNAL_SECURITY_TOKEN_MAX_LIFETIME)
  private val compatibilityMode = InternalSecurityCryptoCompatibilityMode.withName(
    conf.get(INTERNAL_SECURITY_CRYPTO_COMPATIBILITY_MODE))
  private val provider: EngineSecuritySecretProvider = EngineSecuritySecretProvider.create(conf)
  private val (secretKeySpec, encryptor, decryptor) =
    initializeForAuth(cryptoCipher, normalizeSecret(provider.getSecret()))

  private def initializeForAuth(cipher: String, secret: String): (SecretKeySpec, Cipher, Cipher) = {
    val secretKeySpec =
      new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), cryptoKeyAlgorithm)
    val _encryptor = Cipher.getInstance(cipher)
    val _decryptor = Cipher.getInstance(cipher)
    (secretKeySpec, _encryptor, _decryptor)
  }

  def issueToken(): String = {
    val value = KyuubiInternalAccessIdentifier.newIdentifier(tokenMaxLifeTime).toJson
    if (compatibilityMode == InternalSecurityCryptoCompatibilityMode.MIGRATE) {
      encryptLegacy(value)
    } else {
      encrypt(value)
    }
  }

  def authToken(tokenStr: String): Unit = {
    val identifier =
      try {
        parseToken(tokenStr, useLegacyIv = false)
      } catch {
        case _: Exception
            if compatibilityMode != InternalSecurityCryptoCompatibilityMode.STRICT =>
          try {
            parseToken(tokenStr, useLegacyIv = true)
          } catch {
            case _: Exception =>
              throw KyuubiSQLException("Invalid engine access token")
          }
        case _: Exception =>
          throw KyuubiSQLException("Invalid engine access token")
      }
    if (identifier.issueDate + identifier.maxDate < System.currentTimeMillis()) {
      throw KyuubiSQLException("The engine access token is expired")
    }
  }

  private[authentication] def encrypt(value: String): String = {
    val nonce = new Array[Byte](cryptoIvLength)
    InternalSecurityAccessor.random.nextBytes(nonce)
    encrypt(value, nonce, prefixIv = true)
  }

  private def encryptLegacy(value: String): String = {
    encrypt(value, new Array[Byte](cryptoIvLength), prefixIv = false)
  }

  private def encrypt(value: String, iv: Array[Byte], prefixIv: Boolean): String = synchronized {
    encryptor.init(Cipher.ENCRYPT_MODE, secretKeySpec, new IvParameterSpec(iv))
    val encrypted = encryptor.doFinal(value.getBytes(StandardCharsets.UTF_8))
    byteArrayToHexString(if (prefixIv) iv ++ encrypted else encrypted)
  }

  private[authentication] def decrypt(value: String): String = {
    decrypt(value, useLegacyIv = false)
  }

  private def decrypt(value: String, useLegacyIv: Boolean): String = synchronized {
    val bytes = hexStringToByteArray(value)
    val (iv, encrypted) =
      if (useLegacyIv) {
        (new Array[Byte](cryptoIvLength), bytes)
      } else {
        if (bytes.length <= cryptoIvLength) {
          throw new IllegalArgumentException(
            "Malformed engine access token: ciphertext is shorter than the IV length")
        }
        (bytes.take(cryptoIvLength), bytes.drop(cryptoIvLength))
      }
    decryptor.init(Cipher.DECRYPT_MODE, secretKeySpec, new IvParameterSpec(iv))
    new String(decryptor.doFinal(encrypted), StandardCharsets.UTF_8)
  }

  private def parseToken(tokenStr: String, useLegacyIv: Boolean): KyuubiInternalAccessIdentifier = {
    KyuubiInternalAccessIdentifier.fromJson(decrypt(tokenStr, useLegacyIv))
  }

  private def normalizeSecret(secret: String): String = {
    val normalizedSecret = new Array[Char](cryptoKeyLengthBytes)
    val placeHolder = ' '
    for (i <- 0 until cryptoKeyLengthBytes) {
      if (i < secret.length) {
        normalizedSecret.update(i, secret.charAt(i))
      } else {
        normalizedSecret.update(i, placeHolder)
      }
    }
    new String(normalizedSecret)
  }

  private def hexStringToByteArray(str: String): Array[Byte] = {
    // Use a catchable exception instead of assert so authToken can try the legacy format when enabled.
    if (str.length % 2 != 0) {
      throw new IllegalArgumentException(
        "Malformed engine access token: hex string has an odd length")
    }
    val data = new Array[Byte](str.length / 2)
    var i = 0
    while (i < str.length) {
      val highDigit = Character.digit(str.charAt(i), 16)
      val lowDigit = Character.digit(str.charAt(i + 1), 16)
      if (highDigit < 0 || lowDigit < 0) {
        throw new IllegalArgumentException(
          "Malformed engine access token: contains non-hexadecimal characters")
      }
      data.update(i / 2, ((highDigit << 4) + lowDigit).asInstanceOf[Byte])
      i += 2
    }
    data
  }

  private def byteArrayToHexString(bytes: Array[Byte]): String = {
    bytes.map { byte =>
      Integer.toHexString((byte >> 4) & 0xF) + Integer.toHexString(byte & 0xF)
    }.reduce(_ + _)
  }
}

object InternalSecurityAccessor extends Logging {
  @volatile private var _engineSecurityAccessor: InternalSecurityAccessor = _
  private val random: SecureRandom = new SecureRandom()

  def initialize(conf: KyuubiConf, isServer: Boolean): Unit = {
    if (_engineSecurityAccessor == null) {
      _engineSecurityAccessor = new InternalSecurityAccessor(conf, isServer)
    }
  }

  def get(): InternalSecurityAccessor = {
    _engineSecurityAccessor
  }

  @VisibleForTesting
  def reset(): Unit = {
    _engineSecurityAccessor = null
  }
}
