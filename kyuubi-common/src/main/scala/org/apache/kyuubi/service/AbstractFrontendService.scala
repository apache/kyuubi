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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.ServiceState.LATENT

/**
 * A [[AbstractFrontendService]] is an abstraction for fronted service.
 * An frontend service will receive client requests and translate them to serverable operations or
 * backend operations. It also support exposing itself by `ServiceDiscovery` if the concrete
 * frontend service has a Discovery Service as its child.
 *
 * @param name
 */
abstract class AbstractFrontendService(name: String) extends CompositeService(name) {

  val serverable: Serverable

  final def be: BackendService = serverable.backendService

  val discoveryService: Option[Service]

  def checkInitialized(): Unit = if (getServiceState == ServiceState.LATENT) {
    throw new IllegalStateException(
      s"Illegal Service State: $LATENT for getting the connection URL of $getName")
  }

  def connectionUrl: String

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    discoveryService.foreach(addService)
    super.initialize(conf)
  }

}
