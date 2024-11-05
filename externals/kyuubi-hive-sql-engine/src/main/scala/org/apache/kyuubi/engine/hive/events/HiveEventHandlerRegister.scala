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
package org.apache.kyuubi.engine.hive.events

import java.net.InetAddress

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_EVENT_JSON_LOG_PATH, ENGINE_HIVE_EVENT_LOGGERS}
import org.apache.kyuubi.engine.hive.events.handler.HiveJsonLoggingEventHandler
import org.apache.kyuubi.events.{EventHandlerRegister, KyuubiEvent}
import org.apache.kyuubi.events.handler.{EventHandler, HttpLoggingEventHandler}
import org.apache.kyuubi.util.KyuubiHadoopUtils

object HiveEventHandlerRegister extends EventHandlerRegister {

  override protected def createJsonEventHandler(
      kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    val hostName = InetAddress.getLocalHost.getCanonicalHostName
    HiveJsonLoggingEventHandler(
      s"Hive-$hostName",
      ENGINE_EVENT_JSON_LOG_PATH,
      hadoopConf,
      kyuubiConf)
  }

  override protected def createHttpEventHandler(kyuubiConf: KyuubiConf)
      : EventHandler[KyuubiEvent] = {
    HttpLoggingEventHandler(ENGINE_HIVE_EVENT_LOGGERS, kyuubiConf)
  }

  override protected def getLoggers(conf: KyuubiConf): Seq[String] = {
    conf.get(ENGINE_HIVE_EVENT_LOGGERS)
  }
}
