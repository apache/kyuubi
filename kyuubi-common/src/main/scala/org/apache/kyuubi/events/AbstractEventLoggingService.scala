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

import org.apache.kyuubi.service.CompositeService

abstract class AbstractEventLoggingService
  extends CompositeService("EventLogging") {

  import EventLoggingService._

  private val eventLoggers = new ArrayBuffer[EventLogger]()

  def onEvent(event: KyuubiEvent): Unit = {
    eventLoggers.foreach(_.logEvent(event))
  }

  def addEventLogger(logger: EventLogger): Unit = {
    eventLoggers += logger
  }

  override def start(): Unit = synchronized {
    super.start()
    // expose the event logging service only when the loggers successfully start
    _service = Some(this)
  }

  override def stop(): Unit = synchronized {
    _service = None
    super.stop()
  }

}

object EventLoggingService {

  private[events] var _service: Option[AbstractEventLoggingService] = None

  def onEvent(event: KyuubiEvent): Unit = {
    _service.foreach(_.onEvent(event))
  }
}