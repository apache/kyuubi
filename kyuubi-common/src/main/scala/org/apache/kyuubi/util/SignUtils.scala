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
import java.security.{KeyFactory, KeyPair, KeyPairGenerator, PrivateKey, PublicKey, SecureRandom, Signature}
import java.security.interfaces.ECPublicKey
import java.security.spec.{ECGenParameterSpec, X509EncodedKeySpec}
import java.util.Base64

object SignUtils {

  private lazy val ecKeyPairGenerator = {
    val g = KeyPairGenerator.getInstance("EC")
    g.initialize(new ECGenParameterSpec("secp256k1"), new SecureRandom())
    g
  }

  def generateKeyPair(algorithm: String = "EC"): KeyPair = {
    val generator = algorithm match {
      case "EC" =>
        ecKeyPairGenerator
      case _ =>
        throw new RuntimeException(s"algorithm $algorithm not supported for key pair generation")
    }
    generator.generateKeyPair()
  }

  def signWithPrivateKey(
      plainText: String,
      privateKey: PrivateKey,
      algorithm: String = "SHA256withECDSA"): String = {
    val privateSignature = Signature.getInstance(algorithm)
    privateSignature.initSign(privateKey)
    privateSignature.update(plainText.getBytes(StandardCharsets.UTF_8))
    val signature = privateSignature.sign
    Base64.getEncoder.encodeToString(signature)
  }

  def verifySignWithECDSA(
      plainText: String,
      signature: String,
      publicKeyStr: String): Boolean = {
    val pubKeyBytes = Base64.getDecoder.decode(publicKeyStr)
    val publicKey: PublicKey = KeyFactory.getInstance("EC")
      .generatePublic(new X509EncodedKeySpec(pubKeyBytes)).asInstanceOf[ECPublicKey]
    val publicSignature = Signature.getInstance("SHA256withECDSA")
    publicSignature.initVerify(publicKey)
    publicSignature.update(plainText.getBytes(StandardCharsets.UTF_8))
    val signatureBytes = Base64.getDecoder.decode(signature)
    publicSignature.verify(signatureBytes)
  }
}
