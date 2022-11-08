/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.session

import java.io.File
import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.plugin.SessionConfAdvisor

class FileSessionConfAdvisor extends SessionConfAdvisor {
  override def getConfOverlay(
      user: String,
      sessionConf: util.Map[String, String]): util.Map[String, String] = {
    val sessionProfile: String = sessionConf.get(KyuubiConf.SESSION_CONF_PROFILE.key)
    if (sessionProfile == null) {
      Collections.EMPTY_MAP
    }
    val pathName: String = s"conf/kyuubi-session-${sessionProfile}.conf"
    val propsFile: File = new File(pathName)
    Utils.getPropertiesFromFile(Option(propsFile)).asJava
  }
}
