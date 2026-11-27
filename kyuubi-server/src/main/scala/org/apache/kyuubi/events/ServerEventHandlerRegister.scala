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
package org.apache.kyuubi.events

import java.net.InetAddress

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.events.handler.{EventHandler, ServerJsonLoggingEventHandler, ServerKafkaLoggingEventHandler}
import org.apache.kyuubi.events.handler.ServerKafkaLoggingEventHandler.KAFKA_SERVER_EVENT_HANDLER_PREFIX
import org.apache.kyuubi.util.KyuubiHadoopUtils

object ServerEventHandlerRegister extends EventHandlerRegister {

  override protected def createJsonEventHandler(serverConf: KyuubiConf)
      : EventHandler[KyuubiEvent] = {
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(serverConf)
    val hostName = InetAddress.getLocalHost.getCanonicalHostName
    ServerJsonLoggingEventHandler(
      s"server-$hostName",
      SERVER_EVENT_JSON_LOG_PATH,
      hadoopConf,
      serverConf)
  }

  override def createKafkaEventHandler(serverConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    val topic = serverConf.get(SERVER_EVENT_KAFKA_TOPIC).getOrElse {
      throw new IllegalArgumentException(s"${SERVER_EVENT_KAFKA_TOPIC.key} must be configured")
    }
    val closeTimeoutInMs = serverConf.get(SERVER_EVENT_KAFKA_CLOSE_TIMEOUT)
    val kafkaEventHandlerProducerConf =
      serverConf.getAllWithPrefix(KAFKA_SERVER_EVENT_HANDLER_PREFIX, "")
        .filterKeys(
          !List(SERVER_EVENT_KAFKA_TOPIC, SERVER_EVENT_KAFKA_CLOSE_TIMEOUT).map(_.key).contains(_))
    ServerKafkaLoggingEventHandler(
      topic,
      kafkaEventHandlerProducerConf,
      serverConf,
      closeTimeoutInMs)
  }

  override protected def getLoggers(serverConf: KyuubiConf): Seq[String] = {
    serverConf.get(SERVER_EVENT_LOGGERS)
  }

  override protected def asyncEventLogging(serverConf: KyuubiConf): Boolean = {
    serverConf.get(SERVER_EVENT_ASYNC_ENABLED)
  }
}
