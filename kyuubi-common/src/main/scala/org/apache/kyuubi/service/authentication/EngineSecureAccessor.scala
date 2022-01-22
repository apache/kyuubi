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

import java.security.GeneralSecurityException
import javax.crypto.{Cipher, ShortBufferException}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import org.apache.commons.crypto.cipher.{CryptoCipher, CryptoCipherFactory}
import org.apache.commons.crypto.random.{CryptoRandom, CryptoRandomFactory}

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.security.EngineSecureCryptoConf
import org.apache.kyuubi.service.AbstractService

class EngineSecureAccessor(name: String, val isServer: Boolean) extends AbstractService(name) {
  import EngineSecureAccessProvider._

  def this(isServer: Boolean) = this(classOf[EngineSecureAccessor].getName, isServer)

  private var initialized: Boolean = _

  private var cryptoConf: EngineSecureCryptoConf = _

  private var random: CryptoRandom = _
  private var provider: EngineSecureAccessProvider = _

  private var tokenMaxLifeTime: Long = _
  private var encryptor: CryptoCipher = _
  private var decryptor: CryptoCipher = _

  override def initialize(conf: KyuubiConf): Unit = {
    if (!initialized) {
      cryptoConf = new EngineSecureCryptoConf(conf)
      random = CryptoRandomFactory.getCryptoRandom(cryptoConf.toCommonsCryptoConf())

      tokenMaxLifeTime = conf.get(ENGINE_SECURE_ACCESS_TOKEN_MAX_LIFETIME)
      provider = create(conf.get(ENGINE_SECURE_ACCESS_SECRET_PROVIDER_CLASS))
      provider.initialize(conf)

      initializeForAuth(cryptoConf.cipherTransformation, provider.getSecret())

      super.initialize(conf)
      initialized = true
    }
  }

  private def initializeForAuth(cipher: String, secret: String): Unit = {
    val nonce = randomBytes(cryptoConf.encryptionKeyLength / java.lang.Byte.SIZE)
    val secretKeySpec = new SecretKeySpec(secret.getBytes, cryptoConf.keyAlgorithm)
    // commons-crypto currently only supports ciphers that require an initial vector; so
    // create a dummy vector so that we can initialize the ciphers. In the future, if
    // different ciphers are supported, this will have to be configurable somehow.
    val iv = new Array[Byte](cryptoConf.ivLength)
    System.arraycopy(nonce, 0, iv, 0, Math.min(nonce.length, iv.length))

    val _encryptor = CryptoCipherFactory.getCryptoCipher(cipher, cryptoConf.toCommonsCryptoConf())
    _encryptor.init(Cipher.ENCRYPT_MODE, secretKeySpec, new IvParameterSpec(iv))
    encryptor = _encryptor

    val _decryptoer = CryptoCipherFactory.getCryptoCipher(cipher, cryptoConf.toCommonsCryptoConf())
    _decryptoer.init(Cipher.DECRYPT_MODE, secretKeySpec, new IvParameterSpec(iv))
    decryptor = _decryptoer
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
    new String(doCipherOp(Cipher.ENCRYPT_MODE, value.getBytes(), false))
  }

  private[authentication] def decrypt(value: String): String = {
    new String(doCipherOp(Cipher.DECRYPT_MODE, value.getBytes(), false))
  }

  @throws[GeneralSecurityException]
  private def doCipherOp(mode: Int, in: Array[Byte], isFinal: Boolean): Array[Byte] = {
    var cipher: CryptoCipher = null
    mode match {
      case Cipher.ENCRYPT_MODE =>
        cipher = encryptor

      case Cipher.DECRYPT_MODE =>
        cipher = decryptor

      case _ =>
        throw new IllegalArgumentException(String.valueOf(mode))
    }
    require(cipher != null, "Cipher is invalid because of previous error.")
    try {
      var scale = 1
      var result: Array[Byte] = Array.empty

      while (result.isEmpty) {
        val size = in.length * scale
        val buffer = new Array[Byte](size)
        try {
          val outSize = if (isFinal) {
            cipher.doFinal(in, 0, in.length, buffer, 0)
          } else {
            cipher.update(in, 0, in.length, buffer, 0)
          }

          if (outSize != buffer.length) {
            val output = new Array[Byte](outSize)
            System.arraycopy(buffer, 0, output, 0, output.length)
            result = output
          } else {
            result = buffer
          }
        } catch {
          case _: ShortBufferException =>
            // Try again with a bigger buffer.
            scale *= 2
        }
      }
      result
    } catch {
      case ie: InternalError =>
        // The commons-crypto library will throw InternalError if something goes wrong,
        // and leave bad state behind in the Java wrappers, so it's not safe to use them afterwards.
        if (mode == Cipher.ENCRYPT_MODE) {
          this.encryptor = null
        } else {
          this.decryptor = null
        }
        throw ie
    }
  }

  private def randomBytes(count: Int) = {
    val bytes = new Array[Byte](count)
    random.nextBytes(bytes)
    bytes
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
}
