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

package org.apache.kyuubi.service

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kyuubi.config.KyuubiConf

/**
 * A [[Serverable]] is an abstraction for concreting kyuubi server and engines
 * It contains
 *   - one [[AbstractBackendService]] to do the actual operations or interacting with the
 *     downstream services
 *   - [1, n] [[AbstractFrontendService]]s as a list to handle different requests from the
 *     upstream callers, e.g. JDBC clients.
 *
 * @param name the server name
 */
abstract class Serverable(name: String) extends CompositeService(name) {

  private val started = new AtomicBoolean(false)

  val backendService: AbstractBackendService

  val frontendServices: Seq[AbstractFrontendService]

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    addService(backendService)
    frontendServices.foreach(addService)
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    if (!started.getAndSet(true)) {
      super.start()
    }
  }

  protected def stopServer(): Unit

  override def stop(): Unit = synchronized {
    try {
      if (started.getAndSet(false)) {
        super.stop()
      }
    } catch {
      case t: Throwable =>
        warn(s"Error stopping $name ${t.getMessage}", t)
    } finally {
      try {
        stopServer()
      } catch {
        case t: Throwable =>
          warn(s"Error stopping spark ${t.getMessage}", t)
      }
    }
  }
}
