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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.trino.api.v1.ApiRootResource
import org.apache.kyuubi.server.ui.JettyServer
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.util.JavaUtils

/**
 * A frontend service based on RESTful api via HTTP protocol.
 * Note: Currently, it only be used in the Kyuubi Server side.
 */
class KyuubiTrinoFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiTrinoFrontendService") {

  private var server: JettyServer = _

  private val isStarted = new AtomicBoolean(false)

  lazy val host: String = conf.get(FRONTEND_TRINO_BIND_HOST)
    .getOrElse {
      if (conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
        JavaUtils.findLocalInetAddress.getCanonicalHostName
      } else {
        JavaUtils.findLocalInetAddress.getHostAddress
      }
    }

  private lazy val port: Int = conf.get(FRONTEND_TRINO_BIND_PORT)

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    server = JettyServer(
      getName,
      host,
      port,
      conf.get(FRONTEND_TRINO_MAX_WORKER_THREADS),
      conf.get(FRONTEND_TRINO_JETTY_STOP_TIMEOUT),
      conf.get(FRONTEND_JETTY_SEND_VERSION_ENABLED))
    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    conf.get(FRONTEND_ADVERTISED_HOST) match {
      case Some(advertisedHost) => s"$advertisedHost:$port"
      case None => server.getServerUri
    }
  }

  private def startInternal(): Unit = {
    val contextHandler = ApiRootResource.getServletHandler(this)
    server.addHandler(contextHandler)
  }

  override def start(): Unit = synchronized {
    if (!isStarted.get) {
      try {
        server.start()
        isStarted.set(true)
        info(s"$getName has started at ${server.getServerUri}")
        startInternal()
      } catch {
        case e: Exception => throw new KyuubiException(s"Cannot start $getName", e)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (isStarted.getAndSet(false)) {
      server.stop()
    }
    super.stop()
  }

  override val discoveryService: Option[Service] = None
}
