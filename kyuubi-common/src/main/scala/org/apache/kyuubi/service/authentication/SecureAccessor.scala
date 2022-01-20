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

import java.util.ServiceLoader
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

class SecureAccessor(name: String) extends AbstractService(name) {
  import SecureAccessor._

  def this() = this(classOf[SecureAccessor].getName)

  private var providerOpt: Option[SecureAccessProvider] = _
  private var tokenMaxLifeTime: Long = _
  private var encryptCipherInstance: Cipher = _
  private var decryptCipherInstance: Cipher = _

  override def initialize(conf: KyuubiConf): Unit = {
    tokenMaxLifeTime = conf.get(KyuubiConf.ENGINE_ACCESS_TOKEN_MAX_LIFETIME)
    providerOpt = loadProvider()
    providerOpt.foreach(_.initialize(conf))
    super.initialize(conf)
    SecureAccessor._secureAccessor = this
  }

  def supportSecureAccess(): Boolean = {
    providerOpt.exists(_.supportSecureAccess)
  }

  override def start(): Unit = {
    super.start()
    providerOpt.foreach { provider =>
      val (secret, cipher) = provider.getSecretAndCipher()
      val secretKeySpec = new SecretKeySpec(secret.getBytes, cipher)
      encryptCipherInstance = Cipher.getInstance(cipher)
      encryptCipherInstance.init(Cipher.ENCRYPT_MODE, secretKeySpec)
      decryptCipherInstance = Cipher.getInstance(cipher)
      decryptCipherInstance.init(Cipher.DECRYPT_MODE, secretKeySpec)
    }
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
    byteArrayToHexString(encryptCipherInstance.doFinal(value.getBytes))
  }

  private[authentication] def decrypt(value: String): String = {
    new String(decryptCipherInstance.doFinal(hexStringToByteArray(value)))
  }
}

object SecureAccessor extends Logging {
  @volatile private var _secureAccessor: SecureAccessor = _

  def get(): SecureAccessor = {
    _secureAccessor
  }

  def loadProvider(): Option[SecureAccessProvider] = {
    val loader = ServiceLoader.load(classOf[SecureAccessProvider], getClass.getClassLoader)
    loader.iterator().asScala.toSeq.headOption
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
