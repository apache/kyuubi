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

package org.apache.kyuubi.engine.spark.events

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.kyuubi.SparkContextHelper

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkSQLEngine
import org.apache.kyuubi.engine.spark.events.EventLoggingService._service
import org.apache.kyuubi.service.CompositeService

class EventLoggingService(engine: SparkSQLEngine)
  extends CompositeService("EventLogging") {

  private val eventLoggers = new ArrayBuffer[EventLogger]()

  def onEvent(event: KyuubiEvent): Unit = {
    eventLoggers.foreach(_.logEvent(event))
  }

  override def initialize(conf: KyuubiConf): Unit = {
    conf.get(KyuubiConf.ENGINE_EVENT_LOGGERS)
      .map(EventLoggerType.withName)
      .foreach {
        case EventLoggerType.SPARK =>
          eventLoggers += SparkContextHelper.createSparkHistoryLogger(engine.spark.sparkContext)
        case EventLoggerType.JSON =>
          val jsonEventLogger = new JsonEventLogger(engine.engineId)
          addService(jsonEventLogger)
          eventLoggers += jsonEventLogger
        case logger =>
          // TODO: Add more implementations
          throw new IllegalArgumentException(s"Unrecognized event logger: $logger")
    }
    super.initialize(conf)
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

  def onEvent(event: KyuubiEvent): Unit = {
    _service.foreach(_.onEvent(event))
  }
}

