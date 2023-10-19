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

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import org.apache.hadoop.classification.VisibleForTesting

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class InternalSecurityAccessor(conf: KyuubiConf, val isServer: Boolean) {
  val cryptoKeyLengthBytes = conf.get(ENGINE_SECURITY_CRYPTO_KEY_LENGTH) / java.lang.Byte.SIZE
  val cryptoIvLength = conf.get(ENGINE_SECURITY_CRYPTO_IV_LENGTH)
  val cryptoKeyAlgorithm = conf.get(ENGINE_SECURITY_CRYPTO_KEY_ALGORITHM)
  val cryptoCipher = conf.get(ENGINE_SECURITY_CRYPTO_CIPHER_TRANSFORMATION)

  private val tokenMaxLifeTime: Long = conf.get(ENGINE_SECURITY_TOKEN_MAX_LIFETIME)
  private val provider: EngineSecuritySecretProvider = EngineSecuritySecretProvider.create(conf)
  private val (encryptor, decryptor) =
    initializeForAuth(cryptoCipher, normalizeSecret(provider.getSecret()))

  private def initializeForAuth(cipher: String, secret: String): (Cipher, Cipher) = {
    val secretKeySpec = new SecretKeySpec(secret.getBytes, cryptoKeyAlgorithm)
    val nonce = new Array[Byte](cryptoIvLength)
    val iv = new IvParameterSpec(nonce)

    val _encryptor = Cipher.getInstance(cipher)
    _encryptor.init(Cipher.ENCRYPT_MODE, secretKeySpec, iv)

    val _decryptor = Cipher.getInstance(cipher)
    _decryptor.init(Cipher.DECRYPT_MODE, secretKeySpec, iv)

    (_encryptor, _decryptor)
  }

  def issueToken(): String = {
    encrypt(KyuubiInternalAccessIdentifier.newIdentifier(tokenMaxLifeTime).toJson)
  }

  def authToken(tokenStr: String): Unit = {
    val identifier =
      try {
        KyuubiInternalAccessIdentifier.fromJson(decrypt(tokenStr))
      } catch {
        case _: Exception =>
          throw KyuubiSQLException("Invalid engine access token")
      }
    if (identifier.issueDate + identifier.maxDate < System.currentTimeMillis()) {
      throw KyuubiSQLException("The engine access token is expired")
    }
  }

  private[authentication] def encrypt(value: String): String = synchronized {
    byteArrayToHexString(encryptor.doFinal(value.getBytes))
  }

  private[authentication] def decrypt(value: String): String = synchronized {
    new String(decryptor.doFinal(hexStringToByteArray(value)))
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
    val len = str.length
    assert(len % 2 == 0)
    val data = new Array[Byte](len / 2)
    var i = 0
    while (i < len) {
      data.update(
        i / 2,
        ((Character.digit(str.charAt(i), 16) << 4) +
          Character.digit(str.charAt(i + 1), 16)).asInstanceOf[Byte])
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
