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

package org.apache.kyuubi.ha.client

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.service.{AbstractService, FrontendService}

/**
 * A abstract service for service discovery
 *
 * @param name   the name of the service itself
 * @param fe the frontend service to publish for service discovery
 */
abstract class ServiceDiscovery(
    name: String,
    val fe: FrontendService) extends AbstractService(name) {

  protected val gracefulShutdownLatch = new CountDownLatch(1)
  protected val isServerLost = new AtomicBoolean(false)

  /**
   * a pre-defined namespace used to publish the instance of the associate service
   */
  private var _namespace: String = _
  private var _discoveryClient: DiscoveryClient = _

  def namespace: String = _namespace
  def discoveryClient: DiscoveryClient = _discoveryClient

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf

    _namespace = conf.get(HA_NAMESPACE)
    _discoveryClient = DiscoveryClientProvider.createDiscoveryClient(conf)
    discoveryClient.monitorState(this)
    discoveryClient.createClient()

    super.initialize(conf)
  }

  override def start(): Unit = {
    discoveryClient.registerService(conf, namespace, this)
    info(s"Registered $name in namespace ${_namespace}.")
    super.start()
  }

  // stop the server genteelly
  def stopGracefully(isLost: Boolean = false): Unit = {
    var activeSessionCount = fe.be.sessionManager.getActiveUserSessionCount
    while (activeSessionCount > 0) {
      info(s"$activeSessionCount connection(s) are active, delay shutdown")
      Thread.sleep(TimeUnit.SECONDS.toMillis(10))
      activeSessionCount = fe.be.sessionManager.getActiveUserSessionCount
    }
    isServerLost.set(isLost)
    gracefulShutdownLatch.countDown()
    fe.serverable.stop()
  }
}

object ServiceDiscovery extends Logging {

  def supportServiceDiscovery(conf: KyuubiConf): Boolean = {
    val zkEnsemble = conf.get(HA_ADDRESSES)
    zkEnsemble != null && zkEnsemble.nonEmpty
  }
}
