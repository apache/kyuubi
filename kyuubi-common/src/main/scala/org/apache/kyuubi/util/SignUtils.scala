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

import java.nio.charset.StandardCharsets
import java.security.{InvalidParameterException, KeyFactory, KeyPairGenerator, PrivateKey, PublicKey, SecureRandom, Signature}
import java.security.interfaces.ECPublicKey
import java.security.spec.{ECGenParameterSpec, X509EncodedKeySpec}
import java.util.Base64

object SignUtils {
  val KEYPAIR_ALGORITHM_EC: String = "EC"

  private lazy val ecKeyPairGenerator = {
    val g = KeyPairGenerator.getInstance(KEYPAIR_ALGORITHM_EC)
    g.initialize(new ECGenParameterSpec("secp256k1"), new SecureRandom())
    g
  }

  def generateKeyPair(algorithm: String = "EC"): (PrivateKey, PublicKey) = {
    val generator = algorithm.toUpperCase match {
      case "EC" =>
        ecKeyPairGenerator
      case _ =>
        throw new InvalidParameterException(
          s"algorithm $algorithm not supported for key pair generation")
    }
    val keyPair = generator.generateKeyPair()
    (keyPair.getPrivate, keyPair.getPublic)
  }

  def signWithPrivateKey(
      plainText: String,
      privateKey: PrivateKey,
      algorithm: String = "SHA256withECDSA"): String = {
    val privateSignature = Signature.getInstance(algorithm)
    privateSignature.initSign(privateKey)
    privateSignature.update(plainText.getBytes(StandardCharsets.UTF_8))
    val signatureBytes = privateSignature.sign
    Base64.getEncoder.encodeToString(signatureBytes)
  }

  def verifySignWithECDSA(
      plainText: String,
      signatureBase64: String,
      publicKeyBase64: String): Boolean = {
    try {
      val publicKeyBytes = Base64.getDecoder.decode(publicKeyBase64)
      val publicKey: PublicKey = KeyFactory.getInstance(KEYPAIR_ALGORITHM_EC)
        .generatePublic(new X509EncodedKeySpec(publicKeyBytes)).asInstanceOf[ECPublicKey]
      val signatureBytes = Base64.getDecoder.decode(signatureBase64)
      val publicSignature = Signature.getInstance("SHA256withECDSA")
      publicSignature.initVerify(publicKey)
      publicSignature.update(plainText.getBytes(StandardCharsets.UTF_8))
      publicSignature.verify(signatureBytes)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"signature verification failed: publicKeyBase64:$publicKeyBase64" +
            s", signatureBase64:$signatureBase64, plainText:$plainText",
          e)
    }
  }
}
