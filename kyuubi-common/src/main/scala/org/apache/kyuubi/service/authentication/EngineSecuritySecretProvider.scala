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
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.util.reflect.DynConstructors

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

class SimpleEngineSecuritySecretProviderImpl extends EngineSecuritySecretProvider {

  private var _conf: KyuubiConf = _

  override def initialize(conf: KyuubiConf): Unit = _conf = conf

  override def getSecret(): String = {
    _conf.get(SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET).getOrElse {
      throw new IllegalArgumentException(
        s"${SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET.key} must be configured " +
          s"when ${ENGINE_SECURITY_SECRET_PROVIDER.key} is `simple`.")
    }
  }
}

object EngineSecuritySecretProvider {
  def create(conf: KyuubiConf): EngineSecuritySecretProvider = {
    val provider = DynConstructors.builder()
      .impl(conf.get(ENGINE_SECURITY_SECRET_PROVIDER))
      .buildChecked[EngineSecuritySecretProvider]()
      .newInstance(conf)
    provider.initialize(conf)
    provider
  }
}
