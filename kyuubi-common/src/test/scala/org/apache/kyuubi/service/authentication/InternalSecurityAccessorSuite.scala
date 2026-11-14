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
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.InternalSecurityCryptoCompatibilityMode

class InternalSecurityAccessorSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(KyuubiConf.INTERNAL_SECURITY_SECRET_PROVIDER, "simple")
    .set(KyuubiConf.INTERNAL_SECURITY_SECRET_PROVIDER_SIMPLE_SECRET.key, "ENGINE____SECRET")
  private val ciphers = Seq("AES/CBC/PKCS5PADDING", "AES/CTR/NoPadding")

  test("test encrypt/decrypt, issue token/auth token") {
    ciphers.foreach { cipher =>
      val newConf = compatibilityConf(
        cipher,
        InternalSecurityCryptoCompatibilityMode.STRICT)
      val secureAccessor = new InternalSecurityAccessor(newConf, true)
      val value = "tokenToEncrypt"
      val encryptedValue = secureAccessor.encrypt(value)
      val anotherEncryptedValue = secureAccessor.encrypt(value)
      assert(secureAccessor.decrypt(encryptedValue) === value)
      assert(secureAccessor.decrypt(anotherEncryptedValue) === value)

      val ivHexLength = secureAccessor.cryptoIvLength * 2
      assert(encryptedValue.take(ivHexLength) !== anotherEncryptedValue.take(ivHexLength))
      assert(!encryptedValue.startsWith("0" * ivHexLength))

      val token = secureAccessor.issueToken()
      secureAccessor.authToken(token)
      intercept[KyuubiSQLException](secureAccessor.authToken("invalidToken"))
      intercept[KyuubiSQLException](secureAccessor.authToken("0"))
      intercept[KyuubiSQLException](secureAccessor.authToken("gg"))

      val engineSecureAccessor = new InternalSecurityAccessor(newConf, false)
      engineSecureAccessor.authToken(token)
    }
  }

  test("strict compatibility mode rejects legacy zero-IV tokens") {
    ciphers.foreach { cipher =>
      val strictConf = compatibilityConf(
        cipher,
        InternalSecurityCryptoCompatibilityMode.STRICT)
      val strictAccessor = new InternalSecurityAccessor(strictConf, true)
      strictAccessor.authToken(strictAccessor.issueToken())
      intercept[KyuubiSQLException](strictAccessor.authToken(legacyToken(strictConf)))
    }
  }

  test("read compatible mode accepts both formats and issues current tokens") {
    ciphers.foreach { cipher =>
      val readCompatibleConf = compatibilityConf(
        cipher,
        InternalSecurityCryptoCompatibilityMode.READ_COMPATIBLE)
      val readCompatibleAccessor = new InternalSecurityAccessor(readCompatibleConf, true)
      val strictConf = compatibilityConf(
        cipher,
        InternalSecurityCryptoCompatibilityMode.STRICT)
      val strictAccessor = new InternalSecurityAccessor(strictConf, true)

      readCompatibleAccessor.authToken(legacyToken(readCompatibleConf))
      intercept[KyuubiSQLException](readCompatibleAccessor.authToken("0"))
      intercept[KyuubiSQLException](readCompatibleAccessor.authToken("gg"))
      strictAccessor.authToken(readCompatibleAccessor.issueToken())
    }
  }

  test("migrate mode accepts both formats and issues legacy tokens") {
    ciphers.foreach { cipher =>
      val migrateConf = compatibilityConf(
        cipher,
        InternalSecurityCryptoCompatibilityMode.MIGRATE)
      val migrateAccessor = new InternalSecurityAccessor(migrateConf, true)
      val strictConf = compatibilityConf(
        cipher,
        InternalSecurityCryptoCompatibilityMode.STRICT)
      val strictAccessor = new InternalSecurityAccessor(strictConf, true)

      migrateAccessor.authToken(legacyToken(migrateConf))
      migrateAccessor.authToken(strictAccessor.issueToken())
      assertValidLegacyToken(migrateConf, migrateAccessor.issueToken())
    }
  }

  test("legacy compatibility normalizes secrets to the configured key length") {
    Seq(
      ("S", 128),
      ("ENGINE____SECRET", 128),
      ("ENGINE____SECRETX", 128),
      ("ENGINE____SECRET", 256)).foreach { case (secret, keyLength) =>
      ciphers.foreach { cipher =>
        val migrateConf = compatibilityConf(
          cipher,
          InternalSecurityCryptoCompatibilityMode.MIGRATE,
          secret,
          keyLength)
        val migrateAccessor = new InternalSecurityAccessor(migrateConf, true)

        migrateAccessor.authToken(legacyToken(migrateConf))
        assertValidLegacyToken(migrateConf, migrateAccessor.issueToken())
      }
    }
  }

  private def compatibilityConf(
      cipher: String,
      mode: InternalSecurityCryptoCompatibilityMode.InternalSecurityCryptoCompatibilityMode,
      secret: String = "ENGINE____SECRET",
      keyLength: Int = 128): KyuubiConf = {
    conf.clone
      .set(KyuubiConf.INTERNAL_SECURITY_CRYPTO_CIPHER_TRANSFORMATION, cipher)
      .set(KyuubiConf.INTERNAL_SECURITY_CRYPTO_COMPATIBILITY_MODE, mode.toString)
      .set(KyuubiConf.INTERNAL_SECURITY_SECRET_PROVIDER_SIMPLE_SECRET.key, secret)
      .set(KyuubiConf.INTERNAL_SECURITY_CRYPTO_KEY_LENGTH, keyLength)
  }

  private def legacyToken(newConf: KyuubiConf): String = {
    val identifier = KyuubiInternalAccessIdentifier.newIdentifier(
      newConf.get(KyuubiConf.INTERNAL_SECURITY_TOKEN_MAX_LIFETIME))
    legacyEncrypt(newConf, identifier.toJson)
  }

  private def assertValidLegacyToken(newConf: KyuubiConf, token: String): Unit = {
    val identifier = KyuubiInternalAccessIdentifier.fromJson(legacyDecrypt(newConf, token))
    assert(identifier.issueDate + identifier.maxDate >= System.currentTimeMillis())
  }

  private def legacyEncrypt(newConf: KyuubiConf, value: String): String = {
    val cipher = legacyCipher(newConf, Cipher.ENCRYPT_MODE)
    byteArrayToHexString(cipher.doFinal(value.getBytes(StandardCharsets.UTF_8)))
  }

  private def legacyDecrypt(newConf: KyuubiConf, value: String): String = {
    val cipher = legacyCipher(newConf, Cipher.DECRYPT_MODE)
    new String(cipher.doFinal(hexStringToByteArray(value)), StandardCharsets.UTF_8)
  }

  private def legacyCipher(newConf: KyuubiConf, mode: Int): Cipher = {
    val secretKeySpec = new SecretKeySpec(
      legacySecret(newConf).getBytes(StandardCharsets.UTF_8),
      newConf.get(KyuubiConf.INTERNAL_SECURITY_CRYPTO_KEY_ALGORITHM))
    val cipher = Cipher.getInstance(
      newConf.get(KyuubiConf.INTERNAL_SECURITY_CRYPTO_CIPHER_TRANSFORMATION))
    cipher.init(
      mode,
      secretKeySpec,
      new IvParameterSpec(new Array[Byte](newConf.get(
        KyuubiConf.INTERNAL_SECURITY_CRYPTO_IV_LENGTH))))
    cipher
  }

  private def legacySecret(newConf: KyuubiConf): String = {
    val secret =
      newConf.get(KyuubiConf.INTERNAL_SECURITY_SECRET_PROVIDER_SIMPLE_SECRET).get
    val keyLengthBytes =
      newConf.get(KyuubiConf.INTERNAL_SECURITY_CRYPTO_KEY_LENGTH) / java.lang.Byte.SIZE
    val normalizedSecret = new Array[Char](keyLengthBytes)
    val placeHolder = ' '
    for (i <- 0 until keyLengthBytes) {
      if (i < secret.length) {
        normalizedSecret.update(i, secret.charAt(i))
      } else {
        normalizedSecret.update(i, placeHolder)
      }
    }
    new String(normalizedSecret)
  }

  private def hexStringToByteArray(value: String): Array[Byte] = {
    value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
  }

  private def byteArrayToHexString(bytes: Array[Byte]): String = {
    bytes.map(byte => f"${byte & 0xFF}%02x").mkString
  }
}
