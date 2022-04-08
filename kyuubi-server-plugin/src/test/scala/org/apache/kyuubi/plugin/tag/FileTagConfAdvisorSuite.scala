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

package org.apache.kyuubi.plugin.tag

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.plugin.tag.file.FileTagConfAdvisor

class FileTagConfAdvisorSuite extends KyuubiFunSuite {

  val confAdvisor = new FileTagConfAdvisor

  test("tag conf tests") {
    // system
    checkTagConf("user", Map.empty)(Map(
      "kyuubi.system.a" -> "system_a",
      "kyuubi.system.b" -> "system_b"))

    // server
    checkTagConf("user", Map(KyuubiConf.SERVER_NAME.key -> "server01"))(Map(
      "kyuubi.system.a" -> "system_a",
      "kyuubi.system.b" -> "system_b",
      "kyuubi.server.a" -> "server01_a",
      "kyuubi.server.b" -> "server01_b"))

    // user
    checkTagConf("user01", Map.empty)(Map(
      "kyuubi.system.a" -> "system_a",
      "kyuubi.system.b" -> "system_b",
      "kyuubi.user.a" -> "user01_a",
      "kyuubi.user.b" -> "user01_b"))

    // tag
    checkTagConf("user", Map(KyuubiConf.SESSION_TAGS.key -> "tag01"))(Map(
      "kyuubi.system.a" -> "system_a",
      "kyuubi.system.b" -> "system_b",
      "kyuubi.tag.a" -> "tag01_a",
      "kyuubi.tag.b" -> "tag01_b"))

    // override tag conf
    checkTagConf("user", Map(KyuubiConf.SESSION_TAGS.key -> "tag01,tag02"))(Map(
      "kyuubi.system.a" -> "system_a",
      "kyuubi.system.b" -> "system_b",
      "kyuubi.tag.a" -> "tag02_a",
      "kyuubi.tag.b" -> "tag02_b"))
  }

  def checkTagConf(
      user: String,
      sessionConf: Map[String, String])(expected: Map[String, String]): Unit = {
    val conf = confAdvisor.getConfOverlay(user, sessionConf.asJava)
    assertResult(expected)(conf.asScala)
  }

}
