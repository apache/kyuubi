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

import scala.util.Random

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.security.EngineSecureCryptoConf
import org.apache.kyuubi.service.AbstractService

class EngineSecureAccessor(name: String, val isServer: Boolean) extends AbstractService(name) {
  import EngineSecureAccessProvider._
  import EngineSecureAccessor._

  def this(isServer: Boolean) = this(classOf[EngineSecureAccessor].getName, isServer)

  private var initialized: Boolean = _
  private var cryptoConf: EngineSecureCryptoConf = _

  private var provider: EngineSecureAccessProvider = _
  private var tokenMaxLifeTime: Long = _
  private var encryptor: Cipher = _
  private var decryptor: Cipher = _

  override def initialize(conf: KyuubiConf): Unit = {
    if (!initialized) {
      cryptoConf = new EngineSecureCryptoConf(conf)

      tokenMaxLifeTime = conf.get(ENGINE_SECURE_ACCESS_TOKEN_MAX_LIFETIME)
      provider = create(conf.get(ENGINE_SECURE_ACCESS_SECRET_PROVIDER_CLASS))
      provider.initialize(conf)

      initializeForAuth(cryptoConf.cipherTransformation, normalizeSecret(provider.getSecret()))

      super.initialize(conf)
      initialized = true
    }
  }

  private def initializeForAuth(cipher: String, secret: String): Unit = {
    val secretKeySpec = new SecretKeySpec(secret.getBytes, cryptoConf.keyAlgorithm)
    val nonce = new Array[Byte](cryptoConf.ivLength)
    Random.nextBytes(nonce)
    val iv = new IvParameterSpec(nonce)

    encryptor = Cipher.getInstance(cipher)
    encryptor.init(Cipher.ENCRYPT_MODE, secretKeySpec, iv)

    decryptor = Cipher.getInstance(cipher)
    decryptor.init(Cipher.DECRYPT_MODE, secretKeySpec, iv)
  }

  def supportSecureAccess(): Boolean = {
    provider.supportSecureAccess
  }

  def issueToken(): String = {
    if (supportSecureAccess()) {
      encrypt(KyuubiInternalAccessIdentifier.newIdentifier(tokenMaxLifeTime).toJson)
    } else {
      throw new UnsupportedOperationException("Do not support issue secure token")
    }
  }

  def authToken(tokenStr: String): Unit = {
    if (supportSecureAccess()) {
      val identifier = KyuubiInternalAccessIdentifier.fromJson(decrypt(tokenStr))
      if (identifier.issueDate + identifier.maxDate < System.currentTimeMillis()) {
        throw KyuubiSQLException("The engine access token is expired")
      }
    }
  }

  private[authentication] def encrypt(value: String): String = {
    byteArrayToHexString(encryptor.doFinal(value.getBytes))
  }

  private[authentication] def decrypt(value: String): String = {
    new String(decryptor.doFinal(hexStringToByteArray(value)))
  }

  private def normalizeSecret(secret: String): String = {
    val secretLength = cryptoConf.encryptionKeyLength / java.lang.Byte.SIZE
    val normalizedSecret = new Array[Char](secretLength)
    for (i <- 0 until secretLength) {
      if (i < secret.length) {
        normalizedSecret.update(i, secret.charAt(i))
      } else {
        normalizedSecret.update(i, ' ')
      }
    }
    new String(normalizedSecret)
  }
}

object EngineSecureAccessor extends Logging {
  @volatile private var _secureAccessor: EngineSecureAccessor = _

  def get(): EngineSecureAccessor = {
    _secureAccessor
  }

  def getOrCreate(isServer: Boolean): EngineSecureAccessor = {
    if (_secureAccessor == null) {
      _secureAccessor = new EngineSecureAccessor(isServer)
    }
    _secureAccessor
  }

  private def hexStringToByteArray(str: String): Array[Byte] = {
    val len = str.length
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
