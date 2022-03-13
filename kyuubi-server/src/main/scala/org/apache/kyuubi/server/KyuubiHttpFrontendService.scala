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

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.hive.service.rpc.thrift.TCLIService
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TServlet
import org.eclipse.jetty.http.HttpMethod
import org.eclipse.jetty.io.{Connection, EndPoint}
import org.eclipse.jetty.security.{ConstraintMapping, ConstraintSecurityHandler}
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.security.Constraint
import org.eclipse.jetty.util.thread.ExecutorThreadPool

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.{Serverable, Service, TFrontendService}
import org.apache.kyuubi.util.NamedThreadFactory

/**
 * Apache Thrift based hive service rpc
 *  1. the server side implementation serves client-server rpc calls
 *  2. the engine side implementations serve server-engine rpc calls
 */
final class KyuubiHttpFrontendService(
    override val serverable: Serverable)
  extends TFrontendService("KyuubiHttpFrontendService") {

  override protected lazy val serverHost: Option[String] =
    conf.get(FRONTEND_THRIFT_HTTP_BIND_HOST)
  override protected lazy val portNum: Int = conf.get(FRONTEND_THRIFT_HTTP_BIND_PORT)

  private var server: Option[Server] = None

  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target
   * URL to differ, e.g. http://gateway:port/hive2/servlets/thrifths2/.
   * @param conf the configuration of the service
   */
  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    try {
      // Server thread pool
      // Start with minWorkerThreads, expand till maxWorkerThreads and reject
      // subsequent requests
      val minThreads = conf.get(FRONTEND_THRIFT_MIN_WORKER_THREADS)
      val maxThreads = conf.get(FRONTEND_THRIFT_MAX_WORKER_THREADS)
      val keepAliveTime = conf.get(FRONTEND_THRIFT_WORKER_KEEPALIVE_TIME)
      val executor = new ThreadPoolExecutor(
        minThreads,
        maxThreads,
        keepAliveTime,
        TimeUnit.MILLISECONDS,
        new SynchronousQueue[Runnable](),
        new NamedThreadFactory(getName + "HttpHandler-Pool", false))
      val threadPool = new ExecutorThreadPool(executor)
      // HTTP Server
      server = Some(new Server(threadPool))
      val httpConf = new HttpConfiguration
      // Configure header size
      val requestHeaderSize = conf.get(FRONTEND_THRIFT_HTTP_REQUEST_HEADER_SIZE)
      val responseHeaderSize = conf.get(FRONTEND_THRIFT_HTTP_RESPONSE_HEADER_SIZE)
      httpConf.setRequestHeaderSize(requestHeaderSize)
      httpConf.setResponseHeaderSize(responseHeaderSize)
      val connectionFactory = new HttpConnectionFactory(httpConf) {
        override def newConnection(connector: Connector, endPoint: EndPoint): Connection = {
          super.newConnection(connector, endPoint)
        }
      }
      val connector = new ServerConnector(server.get, connectionFactory)
      connector.setPort(portNum)
      // Linux:yes, Windows:no
      connector.setReuseAddress(true)
      val maxIdleTime = conf.get(FRONTEND_THRIFT_HTTP_MAX_IDLE_TIME)
      connector.setIdleTimeout(maxIdleTime)
      connector.setAcceptQueueSize(maxThreads)
      server.foreach(_.addConnector(connector))
      val processor = new TCLIService.Processor[TCLIService.Iface](this)
      val protocolFactory = new TBinaryProtocol.Factory
      val servlet = new TServlet(processor, protocolFactory)
      // Context handler
      val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
      context.setContextPath("/")
      val gzipHandler = new GzipHandler
      gzipHandler.setHandler(context)
      gzipHandler.addIncludedMethods(HttpMethod.POST.asString())
      gzipHandler.addIncludedMimeTypes("application/x-thrift")
      server.foreach(_.setHandler(gzipHandler))
      context.addServlet(
        new ServletHolder(servlet),
        getHttpPath(conf.get(FRONTEND_THRIFT_HTTP_PATH)))
      constrainHttpMethods(context)
      info(s"Initializing $getName on ${serverAddr.getHostName}:${serverSocket.getLocalPort} with" +
        s" [$minThreads, $maxThreads] worker threads")
    } catch {
      case e: Throwable =>
        error(e)
        throw new KyuubiException(
          s"Failed to initialize frontend service on $serverAddr:$portNum.",
          e)
    }
    super.initialize(conf)
  }

  override def run(): Unit =
    try {
      if (isServer()) {
        info(s"Starting and exposing JDBC connection at: jdbc:hive2://$connectionUrl/")
      }
      server.foreach(_.start())
    } catch {
      case _: InterruptedException => error(s"$getName is interrupted")
      case t: Throwable =>
        error(s"Error starting $getName", t)
        System.exit(-1)
    }

  override protected def stopServer(): Unit = {
    server.foreach(_.stop())
    server = None
  }

  override protected def isServer(): Boolean = true

  override val discoveryService: Option[Service] = None

  private def getHttpPath(httpPath: String): String = {
    if (httpPath == null || httpPath == "") return "/*"
    else {
      if (!httpPath.startsWith("/")) return "/" + httpPath
      if (httpPath.endsWith("/")) return httpPath + "*"
      if (!httpPath.endsWith("/*")) return httpPath + "/*"
    }
    httpPath
  }

  def constrainHttpMethods(ctxHandler: ServletContextHandler): Unit = {
    val constraint = new Constraint
    constraint.setAuthenticate(true)
    val cmt = new ConstraintMapping
    cmt.setConstraint(constraint)
    cmt.setMethod("TRACE")
    cmt.setPathSpec("/*")
    val securityHandler = new ConstraintSecurityHandler
    val cmo = new ConstraintMapping
    cmo.setConstraint(constraint)
    cmo.setMethod("OPTIONS")
    cmo.setPathSpec("/*")
    securityHandler.setConstraintMappings(Array[ConstraintMapping](cmt, cmo))
    ctxHandler.setSecurityHandler(securityHandler)
  }
}
