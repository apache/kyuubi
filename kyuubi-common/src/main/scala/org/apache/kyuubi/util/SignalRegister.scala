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

package org.apache.kyuubi.util

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.commons.lang.SystemUtils
import org.slf4j.Logger
import sun.misc.{Signal, SignalHandler}

import org.apache.kyuubi.Logging

object SignalRegister extends Logging {
  private val handlers = new scala.collection.mutable.HashMap[String, ActionHandler]

  def registerLogger(log: Logger): Unit = {
    Seq("TERM", "HUP", "INT").foreach { sig =>
      if (SystemUtils.IS_OS_UNIX) {
        try {
          val handler = handlers.getOrElseUpdate(sig, {
            info(s"Registering signal handler for $sig")
            ActionHandler(new Signal(sig))
          })
          handler.register({
            log.error("RECEIVED SIGNAL " + sig)
            false
          })
        } catch {
          case ex: Exception => warn(s"Failed to register signal handler for $sig", ex)
        }
      }
    }
  }

  case class ActionHandler(signal: Signal) extends SignalHandler {
    private val actions = Collections.synchronizedList(new java.util.LinkedList[() => Boolean])
    private val prevHandler: SignalHandler = Signal.handle(signal, this)

    override def handle(sig: Signal): Unit = {
      Signal.handle(signal, prevHandler)

      val escalate = !actions.asScala.forall(action => action())
      if (escalate) {
        prevHandler.handle(sig)
      }
      Signal.handle(signal, this)
    }
    def register(action: => Boolean): Unit = actions.add(() => action)
  }

}
