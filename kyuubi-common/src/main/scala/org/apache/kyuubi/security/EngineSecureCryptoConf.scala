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

package org.apache.kyuubi.security

import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, KyuubiConf}

class EngineSecureCryptoConf(conf: KyuubiConf) {
  import EngineSecureCryptoConf._

  def encryptionKeyLength: Int = conf.get(ENCRYPTION_KEY_LENGTH)

  def ivLength: Int = conf.get(IV_LENGTH)

  def keyAlgorithm: String = conf.get(KEY_ALGORITHM)

  def cipherTransformation: String = conf.get(CIPHER_TRANSFORMATION)
}

object EngineSecureCryptoConf {
  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val ENCRYPTION_KEY_LENGTH: ConfigEntry[Int] =
    buildConf("engine.secure.crypto.keyLength")
      .doc("The length in bits of the encryption key to generate. " +
        "Valid values are 128, 192 and 256")
      .version("1.5.0")
      .intConf
      .checkValues(Set(128, 192, 256))
      .createWithDefault(128)

  val IV_LENGTH: ConfigEntry[Int] =
    buildConf("engine.secure.crypto.ivLength")
      .doc("Initial vector length, in bytes.")
      .version("1.5.0")
      .intConf
      .createWithDefault(16)

  val KEY_ALGORITHM: ConfigEntry[String] =
    buildConf("engine.secure.crypto.keyAlgorithm")
      .doc("The algorithm for generated secret keys.")
      .version("1.5.0")
      .stringConf
      .createWithDefault("AES")

  val CIPHER_TRANSFORMATION: ConfigEntry[String] =
    buildConf("engine.secure.crypto.cipher")
      .doc("The cipher transformation to use for encrypting engine access token.")
      .version("1.5.0")
      .stringConf
      .createWithDefault("AES/CBC/PKCS5PADDING")
}
