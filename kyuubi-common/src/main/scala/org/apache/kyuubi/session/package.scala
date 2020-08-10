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

package org.apache.kyuubi

import java.time.Duration

import org.apache.kyuubi.config.ConfigEntry
import org.apache.kyuubi.config.KyuubiConf.buildConf

package object session {

  val SESSION_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("session.check.interval")
      .doc("The check interval for frontend session/operation timeout.")
      .timeConf
      .checkValue(_ > Duration.ofSeconds(3).toMillis, "Minimum 3 seconds")
      .createWithDefault(Duration.ofHours(6).toMillis)

  val SESSION_TIMEOUT: ConfigEntry[Long] =
    buildConf("session.timeout")
      .doc("The check interval for frontend session/operation timeout.")
      .timeConf
      .checkValue(_ > Duration.ofSeconds(3).toMillis, "Minimum 3 seconds")
      .createWithDefault(Duration.ofHours(6).toMillis)

}
