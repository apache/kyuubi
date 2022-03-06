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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SECURITY_SECRET_PROVIDER

trait EngineSecuritySecretProvider {

  /**
   * Initialize with kyuubi conf.
   */
  def initialize(conf: KyuubiConf): Unit

  /**
   * Get the secret to encrypt and decrypt the secure access token.
   */
  def getSecret(): String
}

object EngineSecuritySecretProvider {
  def create(conf: KyuubiConf): EngineSecuritySecretProvider = {
    val providerClass = Class.forName(conf.get(ENGINE_SECURITY_SECRET_PROVIDER))
    val provider = providerClass.getConstructor().newInstance()
      .asInstanceOf[EngineSecuritySecretProvider]
    provider.initialize(conf)
    provider
  }
}
