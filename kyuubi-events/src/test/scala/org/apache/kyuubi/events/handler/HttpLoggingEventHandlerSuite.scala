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

import java.time.Duration

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SPARK_EVENT_LOGGERS
import org.apache.kyuubi.events.KyuubiEvent

class HttpLoggingEventHandlerSuite extends KyuubiFunSuite {

  test("testApply") {
    val conf = new KyuubiConf(false)
    conf.set(KyuubiConf.ENGINE_SPARK_EVENT_LOGGERS, Seq("HTTP"))
    conf.set(KyuubiConf.ENGINE_SPARK_EVENT_HTTP_INGEST_URI, "http://localhost:8081/events")
    conf.set(KyuubiConf.ENGINE_SPARK_EVENT_HTTP_RETRY_COUNT, 4)
    conf.set(
      KyuubiConf.ENGINE_SPARK_EVENT_HTTP_CONNECT_TIMEOUT,
      Duration.ofMillis(30 * 1000).toMillis)
    conf.set(
      KyuubiConf.ENGINE_SPARK_EVENT_HTTP_SOCKET_TIMEOUT,
      Duration.ofMillis(2 * 60 * 1000).toMillis)
    conf.set(
      KyuubiConf.ENGINE_SPARK_EVENT_HTTP_HEADERS,
      Seq(
        "Authorization:Basic cHJlc3RvOnByZXN0bzEyMw==",
        "Host:test.tjheywa.yiducloud.cn",
        "Cookie:visit_label=gejianbao"))

    val handler = new HttpLoggingEventHandler(ENGINE_SPARK_EVENT_LOGGERS, conf)

    val event = HttpEvent("a http event", System.currentTimeMillis())
    handler.apply(event)

  }
}

case class HttpEvent(
    name: String,
    createdTime: Long) extends KyuubiEvent {
  override def partitions: Seq[(String, String)] = {
    ("day", Utils.getDateFromTimestamp(createdTime)) :: Nil
  }
}

object HttpEvent {
  def apply(name: String, createdTime: Long): HttpEvent = {
    new HttpEvent(name, createdTime)
  }
}
