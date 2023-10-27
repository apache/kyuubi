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

import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.{ContextHandlerCollection, ErrorHandler}
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.Utils.isWindows
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

private[kyuubi] case class JettyServer(
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
  def getServerUri: String = connector.getHost + ":" + connector.getLocalPort

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

  def getState: String = server.getState
}

object JettyServer extends Logging {

  def apply(name: String, host: String, port: Int, kyuubiConf: KyuubiConf): JettyServer = {

    val poolSize = kyuubiConf.get(FRONTEND_REST_MAX_WORKER_THREADS)
    val pool = new QueuedThreadPool(poolSize)
    pool.setName(name)
    pool.setDaemon(true)

    val server = new Server(pool)

    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(server)
    server.addBean(errorHandler)

    val collection = new ContextHandlerCollection
    server.setHandler(collection)

    val serverExecutor = new ScheduledExecutorScheduler(s"$name-JettyScheduler", true)

    val connector = getServerConnector(server, host, port, serverExecutor, kyuubiConf)

    new JettyServer(server, connector, collection)
  }

  def getServerConnector(
      server: Server,
      host: String,
      port: Int,
      serverExecutor: ScheduledExecutorScheduler,
      conf: KyuubiConf): ServerConnector = {
    val httpConf = new HttpConfiguration()
    val useSsl = conf.get(FRONTEND_REST_USE_SSL)

    val connector = {
      if (useSsl) {
        val keyStorePath = conf.get(FRONTEND_REST_SSL_KEYSTORE_PATH)

        if (keyStorePath.isEmpty) {
          throw new IllegalArgumentException(FRONTEND_REST_SSL_KEYSTORE_PATH.key +
            " Not configured for SSL connection, please set the key with: " +
            FRONTEND_REST_SSL_KEYSTORE_PATH.doc)
        }

        val keyStorePassword = conf.get(FRONTEND_REST_SSL_KEYSTORE_PASSWORD)
        if (keyStorePassword.isEmpty) {
          throw new IllegalArgumentException(FRONTEND_REST_SSL_KEYSTORE_PASSWORD.key +
            " Not configured for SSL connection. please set the key with: " +
            FRONTEND_REST_SSL_KEYSTORE_PASSWORD.doc)
        }

        val sslContextFactory = new SslContextFactory.Server
        val excludedProtocols = conf.get(FRONTEND_REST_SSL_PROTOCOL_BLACKLIST)
        val excludeCipherSuites = conf.get(FRONTEND_REST_SSL_EXCLUDE_CIPHER_SUITES)
        val keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE)
        val keyStoreAlgorithm = conf.get(FRONTEND_SSL_KEYSTORE_ALGORITHM)

        info("Jetty Server SSL: adding excluded protocols: " +
          String.join(",", excludedProtocols: _*))
        sslContextFactory.addExcludeProtocols(excludedProtocols: _*)
        info("Jetty Server SSL: SslContextFactory.getExcludeProtocols = " +
          String.join(",", sslContextFactory.getExcludeProtocols: _*))
        info("Jetty Server SSL: setting excluded cipher Suites: " +
          String.join(",", excludeCipherSuites: _*))
        sslContextFactory.setExcludeCipherSuites(excludeCipherSuites: _*)
        info("Jetty SSL: SslContextFactory.getExcludeCipherSuites = " +
          String.join(",", sslContextFactory.getExcludeCipherSuites: _*))

        sslContextFactory.setKeyStorePath(keyStorePath.get)
        sslContextFactory.setKeyStorePassword(keyStorePassword.get)

        keyStoreType.foreach(sslContextFactory.setKeyStoreType)
        keyStoreAlgorithm.foreach(sslContextFactory.setKeyManagerFactoryAlgorithm)

        new ServerConnector(
          server,
          sslContextFactory,
          new HttpConnectionFactory(httpConf))
      } else {
        new ServerConnector(
          server,
          null,
          serverExecutor,
          null,
          -1,
          -1,
          new HttpConnectionFactory(httpConf))
      }
    }
    connector.setHost(host)
    connector.setPort(port)
    connector.setReuseAddress(!isWindows)
    connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

    connector
  }
}
