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

package org.apache.kyuubi.server.ui

import org.eclipse.jetty.server.{Handler, HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.server.handler.{ContextHandlerCollection, ErrorHandler}
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}

import org.apache.kyuubi.util.JavaUtils

private[kyuubi] class JettyServer(
    server: Server,
    connector: ServerConnector,
    rootHandler: ContextHandlerCollection) {

  def start(): Unit = synchronized {
    try {
      server.start()
      connector.start()
      server.addConnector(connector)
    } catch {
      case e: Exception =>
        stop()
        throw e
    }
  }

  def stop(): Unit = synchronized {
    server.stop()
    connector.stop()
    server.getThreadPool match {
      case lifeCycle: LifeCycle => lifeCycle.stop()
      case _ =>
    }
  }
  val serverUri: String = connector.getHost + ":" + connector.getLocalPort

  def addHandler(handler: Handler): Unit = synchronized {
    rootHandler.addHandler(handler)
    if (!handler.isStarted) handler.start()
  }

  def addStaticHandler(
      resourceBase: String,
      contextPath: String): Unit = {
    addHandler(JettyUtils.createStaticHandler(resourceBase, contextPath))
  }

  def addRedirectHandler(
      src: String,
      dest: String): Unit = {
    addHandler(JettyUtils.createRedirectHandler(src, dest))
  }

  def isStarted: Boolean = server.isStarted
}

object JettyServer {

  def apply(
      name: String,
      host: String,
      port: Int,
      poolSize: Int,
      stopTimeout: Long,
      sendServerVersion: Boolean): JettyServer = {
    val pool = new QueuedThreadPool(poolSize)
    pool.setName(name)
    pool.setDaemon(true)
    val server = new Server(pool)
    server.setStopTimeout(stopTimeout)

    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(server)
    server.addBean(errorHandler)

    val collection = new ContextHandlerCollection
    server.setHandler(collection)

    val serverExecutor = new ScheduledExecutorScheduler(s"$name-JettyScheduler", true)
    val httpConf = new HttpConfiguration()
    httpConf.setSendServerVersion(sendServerVersion)
    val connector = new ServerConnector(
      server,
      null,
      serverExecutor,
      null,
      -1,
      -1,
      new HttpConnectionFactory(httpConf))
    connector.setHost(host)
    connector.setPort(port)
    connector.setReuseAddress(!JavaUtils.isWindows)
    connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

    new JettyServer(server, connector, collection)
  }
}
