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

import java.net.ServerSocket
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import javax.security.sasl.AuthenticationException
import javax.servlet.{ServletContextEvent, ServletContextListener}

import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift.{TCLIService, TOpenSessionReq}
import org.apache.thrift.protocol.TBinaryProtocol
import org.eclipse.jetty.http.HttpMethod
import org.eclipse.jetty.security.{ConstraintMapping, ConstraintSecurityHandler}
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.security.Constraint
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.ExecutorThreadPool

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.metrics.MetricsConstants.{THRIFT_HTTP_CONN_FAIL, THRIFT_HTTP_CONN_OPEN, THRIFT_HTTP_CONN_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.server.http.ThriftHttpServlet
import org.apache.kyuubi.server.http.util.SessionManager
import org.apache.kyuubi.service.{Serverable, Service, ServiceUtils, TFrontendService}
import org.apache.kyuubi.util.NamedThreadFactory

/**
 * Apache Thrift based hive service rpc
 *  1. the server side implementation serves client-server rpc calls
 *  2. the engine side implementations serve server-engine rpc calls
 */
final class KyuubiTHttpFrontendService(
    override val serverable: Serverable)
  extends TFrontendService("KyuubiTHttpFrontendService") {

  override protected lazy val serverHost: Option[String] =
    conf.get(FRONTEND_THRIFT_HTTP_BIND_HOST)
  override protected lazy val portNum: Int = conf.get(FRONTEND_THRIFT_HTTP_BIND_PORT)
  override protected lazy val actualPort: Int = portNum
  override protected lazy val serverSocket: ServerSocket = null

  private var server: Option[Server] = None
  private val APPLICATION_THRIFT = "application/x-thrift"

  override protected def hadoopConf: Configuration = KyuubiServer.getHadoopConf()

  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target
   * URL to differ, e.g. http://gateway:port/hive2/servlets/thrifths2/.
   *
   * @param conf the configuration of the service
   */
  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    if (authFactory.isKerberosEnabled) {
      try {
        authFactory.getValidPasswordAuthMethod
      } catch {
        case _: IllegalArgumentException =>
          throw new AuthenticationException("Kerberos is not supported for thrift http mode")
      }
    }

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
      val connectionFactory = new HttpConnectionFactory(httpConf)

      val useSsl = conf.get(FRONTEND_THRIFT_HTTP_USE_SSL)
      val schemeName = if (useSsl) "https" else "http"

      // Change connector if SSL is used
      val connector =
        if (useSsl) {
          val keyStorePath = conf.get(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PATH)

          if (keyStorePath.isEmpty) {
            throw new IllegalArgumentException(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PATH.key +
              " Not configured for SSL connection, please set the key with: " +
              FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PATH.doc)
          }

          val keyStorePassword = conf.get(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD)
          if (keyStorePassword.isEmpty) {
            throw new IllegalArgumentException(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD.key +
              " Not configured for SSL connection. please set the key with: " +
              FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD.doc)
          }

          val sslContextFactory = new SslContextFactory.Server
          val excludedProtocols = conf.get(FRONTEND_THRIFT_HTTP_SSL_PROTOCOL_BLACKLIST)
          val excludeCipherSuites = conf.get(FRONTEND_THRIFT_HTTP_SSL_EXCLUDE_CIPHER_SUITES)
          val keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE)
          val keyStoreAlgorithm = conf.get(FRONTEND_SSL_KEYSTORE_ALGORITHM)
          info("Thrift HTTP Server SSL: adding excluded protocols: " +
            String.join(",", excludedProtocols: _*))
          sslContextFactory.addExcludeProtocols(excludedProtocols: _*)
          info("Thrift HTTP Server SSL: SslContextFactory.getExcludeProtocols = " +
            String.join(",", sslContextFactory.getExcludeProtocols: _*))
          info("Thrift HTTP Server SSL: setting excluded cipher Suites: " +
            String.join(",", excludeCipherSuites: _*))
          sslContextFactory.setExcludeCipherSuites(excludeCipherSuites: _*)
          info("Thrift HTTP Server SSL: SslContextFactory.getExcludeCipherSuites = " +
            String.join(",", sslContextFactory.getExcludeCipherSuites: _*))
          sslContextFactory.setKeyStorePath(keyStorePath.get)
          sslContextFactory.setKeyStorePassword(keyStorePassword.get)
          keyStoreType.foreach(sslContextFactory.setKeyStoreType)
          keyStoreAlgorithm.foreach(sslContextFactory.setKeyManagerFactoryAlgorithm)
          new ServerConnector(
            server.get,
            sslContextFactory,
            connectionFactory)
        } else {
          new ServerConnector(server.get, connectionFactory)
        }

      connector.setPort(portNum)
      // Linux:yes, Windows:no
      // result of setting the SO_REUSEADDR flag is different on Windows
      // http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
      // without this 2 NN's can start on the same machine and listen on
      // the same port with indeterminate routing of incoming requests to them
      connector.setReuseAddress(!SystemUtils.IS_OS_WINDOWS)
      val maxIdleTime = conf.get(FRONTEND_THRIFT_HTTP_MAX_IDLE_TIME)
      connector.setIdleTimeout(maxIdleTime)
      connector.setAcceptQueueSize(maxThreads)
      server.foreach(_.addConnector(connector))

      val processor = new TCLIService.Processor[TCLIService.Iface](this)
      val protocolFactory = new TBinaryProtocol.Factory
      val servlet = new ThriftHttpServlet(processor, protocolFactory, authFactory, conf)
      servlet.init()

      // Context handler
      val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
      context.setContextPath("/")

      context.addEventListener(new ServletContextListener() {
        override def contextInitialized(servletContextEvent: ServletContextEvent): Unit = {
          MetricsSystem.tracing { ms =>
            ms.incCount(THRIFT_HTTP_CONN_TOTAL)
            ms.incCount(THRIFT_HTTP_CONN_OPEN)
          }
        }

        override def contextDestroyed(servletContextEvent: ServletContextEvent): Unit = {
          MetricsSystem.tracing { ms =>
            ms.decCount(THRIFT_HTTP_CONN_OPEN)
          }
        }
      })

      val httpPath = getHttpPath(conf.get(FRONTEND_THRIFT_HTTP_PATH))

      if (conf.get(FRONTEND_THRIFT_HTTP_COMPRESSION_ENABLED)) {
        val gzipHandler = new GzipHandler
        gzipHandler.setHandler(context)
        gzipHandler.addIncludedMethods(HttpMethod.POST.asString())
        gzipHandler.addIncludedMimeTypes(APPLICATION_THRIFT)
        server.foreach(_.setHandler(gzipHandler))
      } else {
        server.foreach(_.setHandler(context))
      }

      context.addServlet(new ServletHolder(servlet), httpPath)
      constrainHttpMethods(context)

      info(s"Started ${getClass.getSimpleName} in $schemeName mode on port $portNum " +
        s"path=$httpPath with $minThreads ... $maxThreads threads")
    } catch {
      case e: Throwable =>
        MetricsSystem.tracing(_.incCount(THRIFT_HTTP_CONN_FAIL))
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

  override protected def getIpAddress: String = {
    Option(SessionManager.getProxyHttpHeaderIpAddress).getOrElse(SessionManager.getIpAddress)
  }

  override protected def getRealUserAndSessionUser(req: TOpenSessionReq): (String, String) = {
    val realUser = getShortName(Option(SessionManager.getUserName).getOrElse(req.getUsername))
    // using the remote ip address instead of that in proxy http header for authentication
    val ipAddress: String = SessionManager.getIpAddress
    val sessionUser: String = getProxyUser(req.getConfiguration, ipAddress, realUser)
    debug(s"Client's real user: $realUser, session user: $sessionUser")
    realUser -> sessionUser
  }

  private def getShortName(userName: String): String = {
    var ret: String = null

    if (userName != null) {
      val indexOfDomainMatch = ServiceUtils.indexOfDomainMatch(userName)
      ret = if (indexOfDomainMatch <= 0) userName else userName.substring(0, indexOfDomainMatch)
    }

    ret
  }
}
