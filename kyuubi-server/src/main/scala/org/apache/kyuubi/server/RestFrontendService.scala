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

import java.net.InetAddress

import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.server.handler.ErrorHandler
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_REST_BIND_HOST, FRONTEND_REST_BIND_PORT}
import org.apache.kyuubi.server.api.ApiUtils
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}

/**
 * A frontend service based on RESTful api via HTTP protocol.
 * Note: Currently, it only be used in the Kyuubi Server side.
 */
private[server] class RestFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("RestFrontendService") with Logging {

  var serverAddr: InetAddress = _
  var portNum: Int = _
  var jettyServer: Server = _
  var connector: ServerConnector = _

  @volatile protected var isStarted = false

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val serverHost = conf.get(FRONTEND_REST_BIND_HOST)
    serverAddr = serverHost.map(InetAddress.getByName).getOrElse(Utils.findLocalInetAddress)
    portNum = conf.get(FRONTEND_REST_BIND_PORT)

    jettyServer = new Server()

    // set error handler
    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(jettyServer)
    jettyServer.addBean(errorHandler)

    jettyServer.setHandler(ApiUtils.getServletHandler(serverable.backendService))

    connector = new ServerConnector(
      jettyServer,
      null,
      new ScheduledExecutorScheduler(s"$getName-JettyScheduler", true),
      null,
      -1,
      -1,
      Array(new HttpConnectionFactory(new HttpConfiguration())): _*)
    connector.setPort(portNum)
    connector.setHost(serverAddr.getCanonicalHostName)
    connector.setReuseAddress(!Utils.isWindows)
    connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    s"${serverAddr.getCanonicalHostName}:$portNum"
  }

  override def start(): Unit = {
    if (!isStarted) {
      try {
        connector.start()
        jettyServer.start()
        info(s"Rest frontend service jetty server has started at ${jettyServer.getURI}.")
      } catch {
        case rethrow: Exception =>
          stopHttpServer()
          throw new KyuubiException("Cannot start rest frontend service jetty server", rethrow)
      }
      isStarted = true
    }

    super.start()
  }

  override def stop(): Unit = {
    if (isStarted) {
      stopHttpServer()
      isStarted = false
    }
    super.stop()
  }

  private def stopHttpServer(): Unit = {
    if (jettyServer != null) {
      try {
        connector.stop()
        info("Rest frontend service server connector has stopped.")
      } catch {
        case err: Exception =>
          error("Cannot safely stop rest frontend service server connector", err)
      } finally {
        connector = null
      }

      try {
        jettyServer.stop()
        info("Rest frontend service jetty server has stopped.")
      } catch {
        case err: Exception => error("Cannot safely stop rest frontend service jetty server", err)
      } finally {
        jettyServer = null
      }
    }
  }

  override val discoveryService: Option[Service] = None
}
