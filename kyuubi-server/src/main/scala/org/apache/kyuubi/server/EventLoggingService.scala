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

package org.apache.kyuubi.server

import java.net.InetAddress

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUDIT_LOG_ENABLE
import org.apache.kyuubi.config.KyuubiConf.SERVER_AUDIT_EVENT_JSON_LOG_PATH
import org.apache.kyuubi.config.KyuubiConf.SERVER_EVENT_JSON_LOG_PATH
import org.apache.kyuubi.config.KyuubiConf.SERVER_EVENT_LOGGERS
import org.apache.kyuubi.events.AbstractEventLoggingService
import org.apache.kyuubi.events.EventLogger
import org.apache.kyuubi.events.EventLoggerType
import org.apache.kyuubi.events.JsonEventLogger
import org.apache.kyuubi.events.KyuubiServerEvent
import org.apache.kyuubi.server.EventLoggingService._service
import org.apache.kyuubi.util.KyuubiHadoopUtils

class EventLoggingService extends AbstractEventLoggingService[KyuubiServerEvent] {

  private lazy val auditLoggers = new ArrayBuffer[EventLogger[KyuubiServerEvent]]()

  private lazy val hostName = InetAddress.getLocalHost.getCanonicalHostName

  def onAuditEvent(event: KyuubiServerEvent): Unit = {
    auditLoggers.foreach(_.logEvent(event))
  }

  override def initialize(conf: KyuubiConf): Unit = {
    // load server event logger
    conf.get(SERVER_EVENT_LOGGERS)
      .map(EventLoggerType.withName)
      .foreach{
        case EventLoggerType.JSON =>
          val jsonEventLogger = new JsonEventLogger[KyuubiServerEvent](s"server-$hostName",
            SERVER_EVENT_JSON_LOG_PATH, new Configuration())
          // TODO: #1180 kyuubiServerEvent need create logRoot automatically
          jsonEventLogger.createEventLogRootDir(conf, KyuubiHadoopUtils.newHadoopConf(conf))
          addService(jsonEventLogger)
          addEventLogger(jsonEventLogger)
        case logger =>
          // TODO: Add more implementations
          throw new IllegalArgumentException(s"Unrecognized event logger: $logger")
      }

    // load audit logger
    if (conf.get(AUDIT_LOG_ENABLE)) {
      val jsonAuditLogger = new JsonEventLogger[KyuubiServerEvent](s"server-$hostName",
        SERVER_AUDIT_EVENT_JSON_LOG_PATH, new Configuration())
      jsonAuditLogger.createEventLogRootDir(conf, KyuubiHadoopUtils.newHadoopConf(conf))
      addService(jsonAuditLogger)
      addAuditEventLogger(jsonAuditLogger)
    }

    super.initialize(conf)
  }

  private def addAuditEventLogger(logger: EventLogger[KyuubiServerEvent]): Unit = {
    auditLoggers += logger
  }

  override def start(): Unit = {
    _service = Some(this)
    super.start()
  }

  override def stop(): Unit = {
    _service = None
    super.stop()
  }

}

object EventLoggingService {

  private var _service: Option[EventLoggingService] = None

  def onEvent(event: KyuubiServerEvent): Unit = {
    _service.foreach(_.onEvent(event))
  }

  def onAuditEvent(event: KyuubiServerEvent): Unit = {
    _service.foreach(_.onAuditEvent(event))
  }
}
