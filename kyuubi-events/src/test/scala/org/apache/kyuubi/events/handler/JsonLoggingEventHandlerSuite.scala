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

package org.apache.kyuubi.events.handler

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_EVENT_JSON_LOG_PATH, EVENT_JSON_LOG_MANAGE_PERMISSIONS_ENABLED}
import org.apache.kyuubi.events.KyuubiEvent

class JsonLoggingEventHandlerSuite extends KyuubiFunSuite {

  test("manage json event log file permissions by default") {
    val logRoot = createLogRoot()
    val conf = KyuubiConf().set(ENGINE_EVENT_JSON_LOG_PATH, logRoot)
    val handler = new JsonLoggingEventHandler(
      "test-log",
      ENGINE_EVENT_JSON_LOG_PATH,
      new Configuration(),
      conf)

    try {
      handler(JsonLogPermissionTestEvent("managed"))
    } finally {
      handler.close()
    }

    val fs = FileSystem.get(new java.net.URI(logRoot), new Configuration())
    val logFile = new Path(logRoot, "json_log_permission_test/day=2026-05-06/test-log.json")
    assert(fs.exists(logFile))
    val expectedPermission = Integer.parseInt("660", 8)
    val actualPermission = fs.getFileStatus(logFile).getPermission.toShort &
      Integer.parseInt("777", 8)
    assert(actualPermission == expectedPermission)
  }

  test("write json event logs without managing permissions") {
    val logRoot = createLogRoot()
    val conf = KyuubiConf()
      .set(ENGINE_EVENT_JSON_LOG_PATH, logRoot)
      .set(EVENT_JSON_LOG_MANAGE_PERMISSIONS_ENABLED, false)
    val handler = new JsonLoggingEventHandler(
      "test-log",
      ENGINE_EVENT_JSON_LOG_PATH,
      new Configuration(),
      conf)

    try {
      handler(JsonLogPermissionTestEvent("unmanaged"))
    } finally {
      handler.close()
    }

    val fs = FileSystem.get(new java.net.URI(logRoot), new Configuration())
    val eventPath = new Path(logRoot, "json_log_permission_test/day=2026-05-06")
    val logFile = new Path(eventPath, "test-log.json")
    assert(fs.getFileStatus(eventPath).isDirectory)
    assert(fs.getFileStatus(logFile).isFile)
  }

  private def createLogRoot(): String = {
    Utils.createTempDir("kyuubi-json-log").toUri.toString
  }
}

case class JsonLogPermissionTestEvent(message: String) extends KyuubiEvent {
  override def partitions: Seq[(String, String)] = Seq("day" -> "2026-05-06")
}
