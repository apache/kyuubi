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

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf

class NoopServer extends Serverable("noop") {

  private val OOMHook = new Runnable { override def run(): Unit = stop() }

  override val backendService = new NoopBackendService
  protected val frontendService: AbstractFrontendService =
    new ThriftFrontendService(backendService, OOMHook)

  override def initialize(conf: KyuubiConf): Unit = {
    addService(frontendService)
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    if (getConf.getOption("kyuubi.test.server.should.fail").exists(_.toBoolean)) {
      throw new IllegalArgumentException("should fail")
    }
  }

  override protected def stopServer(): Unit = {
    throw new KyuubiException("no need to stop me")
  }

  override val discoveryService: Service = backendService
  override protected val supportsServiceDiscovery: Boolean = false

  override def connectionUrl: String = frontendService.connectionUrl()
}
