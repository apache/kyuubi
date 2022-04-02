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

package org.apache.kyuubi.engine.trino

import java.time.Duration

import org.apache.kyuubi.config.ConfigBuilder
import org.apache.kyuubi.config.ConfigEntry
import org.apache.kyuubi.config.KyuubiConf

object TrinoConf {
  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val DATA_PROCESSING_POOL_SIZE: ConfigEntry[Int] =
    buildConf("kyuubi.trino.client.data.processing.pool.size")
      .doc("The size of the thread pool used by the trino client to processing data")
      .version("1.5.0")
      .intConf
      .createWithDefault(3)

  val CLIENT_REQUEST_TIMEOUT: ConfigEntry[Long] =
    buildConf("kyuubi.trino.client.request.timeout")
      .doc("Timeout for Trino client request to trino cluster")
      .version("1.5.0")
      .timeConf
      .createWithDefault(Duration.ofMinutes(2).toMillis)
}
