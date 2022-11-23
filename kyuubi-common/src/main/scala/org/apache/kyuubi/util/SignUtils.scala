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
import java.security.{KeyPairGenerator, PrivateKey, PublicKey, SecureRandom, Signature}
import java.util.Base64

object SignUtils {
  private lazy val rsaKeyPairGenerator = {
    val g = KeyPairGenerator.getInstance("RSA")
    g.initialize(1024, new SecureRandom())
    g
  }

  def generateRSAKeyPair: (PublicKey, PrivateKey) = {
    val keyPair = rsaKeyPairGenerator.generateKeyPair()
    (keyPair.getPublic, keyPair.getPrivate)
  }

  def signWithRSA(plainText: String, privateKey: PrivateKey): String = {
    val privateSignature = Signature.getInstance("SHA256withRSA")
    privateSignature.initSign(privateKey)
    privateSignature.update(plainText.getBytes(StandardCharsets.UTF_8))
    val signature = privateSignature.sign
    Base64.getEncoder.encodeToString(signature)
  }

}
