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

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_EVENT_LOGGERS, ENGINE_TYPE, SERVER_EVENT_LOGGERS}
import org.apache.kyuubi.engine.EngineType
import org.apache.kyuubi.events.EventLoggerType.EventLoggerType
import org.apache.kyuubi.events.handler.EventHandler

trait EventHandlerRegister extends Logging {

  def registerEngineEventLoggers(conf: KyuubiConf): Unit = {
    val engineType = conf.get(ENGINE_TYPE)
    val loggers =
      if (!EngineType.SPARK_SQL.toString.equalsIgnoreCase(engineType)) {
        conf.get(ENGINE_EVENT_LOGGERS).filter(!EventLoggerType.SPARK.toString.equalsIgnoreCase(_))
      } else {
        conf.get(ENGINE_EVENT_LOGGERS)
      }

    register(loggers, conf)
  }

  def registerServerEventLoggers(conf: KyuubiConf): Unit = {
    val loggers = conf.get(SERVER_EVENT_LOGGERS)
    register(loggers, conf)
  }

  private def register(loggers: Seq[String], conf: KyuubiConf): Unit = {
    loggers
      .map(EventLoggerType.withName)
      .foreach { logger =>
        EventBus.register(loadEventHandler(logger, conf))
      }
  }

  protected def createSparkEventHandler(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    throw new KyuubiException(s"Unsupported spark event logger.")
  }

  protected def createJsonEventHandler(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    throw new KyuubiException(s"Unsupported json event logger.")
  }

  protected def createJdbcEventHandler(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    throw new KyuubiException(s"Unsupported jdbc event logger.")
  }

  private def loadEventHandler(
      eventLoggerType: EventLoggerType,
      kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    eventLoggerType match {
      case EventLoggerType.SPARK =>
        createSparkEventHandler(kyuubiConf)

      case EventLoggerType.JSON =>
        createJsonEventHandler(kyuubiConf)

      case EventLoggerType.JDBC =>
        createJdbcEventHandler(kyuubiConf)

      case other =>
        throw new KyuubiException(s"Unsupported event logger: ${other.toString}")
    }
  }
}
