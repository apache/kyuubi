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
import java.util.Base64
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import javax.security.sasl.AuthenticationException
import javax.servlet.{ServletContextEvent, ServletContextListener}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.http.HttpMethod
import org.eclipse.jetty.security.{ConstraintMapping, ConstraintSecurityHandler}
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.security.Constraint
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.ExecutorThreadPool

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException}
import org.apache.kyuubi.cli.Handle
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID, KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET, KYUUBI_SESSION_ENGINE_LAUNCH_SUPPORT_RESULT}
import org.apache.kyuubi.metrics.MetricsConstants.{THRIFT_HTTP_CONN_FAIL, THRIFT_HTTP_CONN_OPEN, THRIFT_HTTP_CONN_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.server.http.ThriftHttpServlet
import org.apache.kyuubi.server.http.authentication.AuthenticationFilter
import org.apache.kyuubi.service.{Serverable, Service, ServiceUtils, TFrontendService}
import org.apache.kyuubi.service.TFrontendService.{CURRENT_SERVER_CONTEXT, OK_STATUS}
import org.apache.kyuubi.session.KyuubiSessionImpl
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TCLIService, TOpenSessionReq, TOpenSessionResp}
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.util.{NamedThreadFactory, SSLUtils}

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

  private var keyStorePath: Option[String] = None
  private var keyStorePassword: Option[String] = None
  private var keyStoreType: Option[String] = None

  private var server: Option[Server] = None
  private val APPLICATION_THRIFT = "application/x-thrift"

  override protected def hadoopConf: Configuration = KyuubiServer.getHadoopConf()

  private lazy val defaultFetchSize = conf.get(KYUUBI_SERVER_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE)

  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target
   * URL to differ, e.g. http://gateway:port/hive2/servlets/thrifths2/.
   *
   * @param conf the configuration of the service
   */
  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    if (authFactory.kerberosEnabled && authFactory.effectivePlainAuthType.isEmpty) {
      throw new AuthenticationException("Kerberos is not supported for Thrift HTTP mode")
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
      val jettySendVersionEnabled = conf.get(FRONTEND_JETTY_SEND_VERSION_ENABLED)
      httpConf.setRequestHeaderSize(requestHeaderSize)
      httpConf.setResponseHeaderSize(responseHeaderSize)
      httpConf.setSendServerVersion(jettySendVersionEnabled)
      val connectionFactory = new HttpConnectionFactory(httpConf)

      val useSsl = conf.get(FRONTEND_THRIFT_HTTP_USE_SSL)
      val schemeName = if (useSsl) "https" else "http"

      // Change connector if SSL is used
      val connector =
        if (useSsl) {
          keyStorePath = conf.get(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PATH)

          if (keyStorePath.isEmpty) {
            throw new IllegalArgumentException(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PATH.key +
              " Not configured for SSL connection, please set the key with: " +
              FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PATH.doc)
          }

          keyStorePassword = conf.get(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD)
          if (keyStorePassword.isEmpty) {
            throw new IllegalArgumentException(FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD.key +
              " Not configured for SSL connection. please set the key with: " +
              FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD.doc)
          }

          val sslContextFactory = new SslContextFactory.Server
          val excludedProtocols = conf.get(FRONTEND_THRIFT_HTTP_SSL_PROTOCOL_BLACKLIST)
          val excludeCipherSuites = conf.get(FRONTEND_THRIFT_HTTP_SSL_EXCLUDE_CIPHER_SUITES)
          keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE)
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

  /** Same as KyuubiTBinaryFrontendService, to return launch engine op handle. */
  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)

      val respConfiguration = new java.util.HashMap[String, String]()
      val launchEngineOp = be.sessionManager.getSession(sessionHandle)
        .asInstanceOf[KyuubiSessionImpl].launchEngineOp

      val opHandleIdentifier = Handle.toTHandleIdentifier(launchEngineOp.getHandle.identifier)
      respConfiguration.put(
        KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID,
        Base64.getEncoder.encodeToString(opHandleIdentifier.getGuid))
      respConfiguration.put(
        KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET,
        Base64.getEncoder.encodeToString(opHandleIdentifier.getSecret))

      respConfiguration.put(KYUUBI_SESSION_ENGINE_LAUNCH_SUPPORT_RESULT, true.toString)

      // HIVE-23005(4.0.0), Hive JDBC driver supposes that server always returns this conf
      respConfiguration.put(
        "hive.server2.thrift.resultset.default.fetch.size",
        defaultFetchSize.toString)

      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      resp.setConfiguration(respConfiguration)
      resp.setStatus(OK_STATUS)
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        error("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
    }
    resp
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
    Option(AuthenticationFilter.getUserProxyHeaderIpAddress).getOrElse(
      AuthenticationFilter.getUserIpAddress)
  }

  override protected def getProxyUser(
      sessionConf: java.util.Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    Option(AuthenticationFilter.getProxyUserName) match {
      case Some(proxyUser) =>
        val proxyUserConf = Map(PROXY_USER.key -> proxyUser)
        super.getProxyUser(
          (sessionConf.asScala ++ proxyUserConf).asJava,
          ipAddress,
          realUser)
      case None => super.getProxyUser(sessionConf, ipAddress, realUser)
    }
  }

  override protected def getRealUserAndSessionUser(req: TOpenSessionReq): (String, String) = {
    val realUser = getShortName(Option(AuthenticationFilter.getUserName)
      .getOrElse(req.getUsername))
    // using the remote ip address instead of that in proxy http header for authentication
    val ipAddress: String = AuthenticationFilter.getUserIpAddress
    val sessionUser: String = if (req.getConfiguration == null) {
      realUser
    } else {
      getProxyUser(req.getConfiguration, ipAddress, realUser)
    }
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

  override def start(): Unit = {
    super.start()
    SSLUtils.tracingThriftSSLCertExpiration(keyStorePath, keyStorePassword, keyStoreType)
  }
}
