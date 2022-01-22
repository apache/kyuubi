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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class EngineSecureAccessorSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
  conf.set(ENGINE_SECURE_ACCESS_SECRET_PROVIDER_CLASS,
    classOf[UserDefinedEngineSecureAccessProvider].getCanonicalName)
  conf.set(ENGINE_SECURE_ENCRYPTION_KEY_SIZE_BYTES, 16)
  conf.set(ENGINE_SECURE_ENCRYPTION_CIPHER_TRANSFORMATION, "AES/CBC/PKCS5Padding")

  test("test encrypt and decrypt") {
    val secureAccessor = new EngineSecureAccessor(true)
    secureAccessor.initialize(conf)
    secureAccessor.start()
    val value = "tokenToEncrypt"
    val encryptedValue = secureAccessor.encrypt(value)
    assert(secureAccessor.decrypt(encryptedValue) === value)
  }

  test("test issue token and auth token") {
    val secureAccessor = new EngineSecureAccessor(true)
    secureAccessor.initialize(conf)
    secureAccessor.start()
    val token = secureAccessor.issueToken()
    secureAccessor.authToken(token)
  }
}
