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

import java.security.InvalidParameterException
import java.util.Base64

import org.apache.kyuubi.KyuubiFunSuite

class SignUtilsSuite extends KyuubiFunSuite {
  test("generate key pair") {
    val keyPair = SignUtils.generateKeyPair("EC")
    assert(keyPair.getPublic !== null)
    assert(keyPair.getPrivate !== null)

    val invalidAlgorithm = "invalidAlgorithm"
    val e1 = intercept[InvalidParameterException](SignUtils.generateKeyPair(invalidAlgorithm))
    assert(e1.getMessage == s"algorithm $invalidAlgorithm not supported for key pair generation")
  }

  test("sign/verify with key pair") {
    val keyPair = SignUtils.generateKeyPair()
    val plainText = "plainText"
    val signature = SignUtils.signWithPrivateKey(plainText, keyPair.getPrivate, "SHA256withECDSA")
    val publicKeyStr = Base64.getEncoder.encodeToString(keyPair.getPublic.getEncoded)
    assert(SignUtils.verifySignWithECDSA(plainText, signature, publicKeyStr))
  }
}
