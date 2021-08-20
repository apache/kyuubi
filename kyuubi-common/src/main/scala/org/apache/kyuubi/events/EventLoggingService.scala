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

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.EventLoggerType.EventLoggerType
import org.apache.kyuubi.service.CompositeService

abstract class AbstractEventLoggingService[T <: KyuubiEvent]
  extends CompositeService("EventLogging") {

  private val eventLoggers = new ArrayBuffer[EventLogger[T]]()

  /**
   * Get configured event log type.
   */
  protected def getLoggers(conf: KyuubiConf): Seq[EventLoggerType]

  def onEvent(event: T): Unit = {
    eventLoggers.foreach(_.logEvent(event))
  }

  override def initialize(conf: KyuubiConf): Unit = {
    getLoggers(conf).foreach(analyseEventLoggerType)
    super.initialize(conf)
  }

  /**
   * Analyse event logger and add to {@link eventLoggers}.
   */
  def analyseEventLoggerType: EventLoggerType => Unit

  def addEventLogger(logger: EventLogger[T]): Unit = {
    eventLoggers += logger
  }
}
