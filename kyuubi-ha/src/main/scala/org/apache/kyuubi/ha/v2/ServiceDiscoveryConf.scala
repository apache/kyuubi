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

package org.apache.kyuubi.ha.v2

import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, KyuubiConf, OptionalConfigEntry}

object ServiceDiscoveryConf {

  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val ENGINE_PROVIDER_STRATEGY: ConfigEntry[String] = buildConf("ha.engine.provider.strategy")
    .version("1.3.0")
    .stringConf
    .createWithDefault("random")

  val ENGINE_PROVIDER_TAGS: OptionalConfigEntry[String] = buildConf("ha.engine.provider.tags")
    .version("1.3.0")
    .stringConf
    .createOptional


  val ENGINE_PROVIDER_VERSION: OptionalConfigEntry[String] = buildConf("ha.engine.provider.version")
    .version("1.3.0")
    .stringConf
    .createOptional

}
