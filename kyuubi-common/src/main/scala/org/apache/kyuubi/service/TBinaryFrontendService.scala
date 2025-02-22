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

import java.net.ServerSocket
import java.security.KeyStore
import java.util.Locale
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import javax.net.ssl.{KeyManagerFactory, SSLServerSocket}

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.shaded.thrift.server.{TServer, TThreadPoolServer}
import org.apache.kyuubi.shaded.thrift.transport.{TServerSocket, TSSLTransportFactory}
import org.apache.kyuubi.util.NamedThreadFactory

/**
 * Apache Thrift based hive service rpc
 *  1. the server side implementation serves client-server rpc calls
 *  2. the engine side implementations serve server-engine rpc calls
 */
abstract class TBinaryFrontendService(name: String)
  extends TFrontendService(name) with TCLIService.Iface with Runnable with Logging {

  import KyuubiConf._

  /**
   * @note this is final because we don't want new implementations for engine to override this.
   *       and we shall simply set it to zero for randomly picking an available port
   */
  final override protected lazy val serverHost: Option[String] =
    conf.get(FRONTEND_THRIFT_BINARY_BIND_HOST)
  final override protected lazy val portNum: Int = conf.get(FRONTEND_THRIFT_BINARY_BIND_PORT)

  protected var server: Option[TServer] = None
  private var _actualPort: Int = _
  override protected lazy val actualPort: Int = _actualPort

  protected var keyStorePath: Option[String] = None
  protected var keyStorePassword: Option[String] = None
  protected var keyStoreType: Option[String] = None

  // Removed OOM hook since Kyuubi #1800 to respect the hive server2 #2383

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    try {
      val minThreads = conf.get(FRONTEND_THRIFT_MIN_WORKER_THREADS)
      val maxThreads = conf.get(FRONTEND_THRIFT_MAX_WORKER_THREADS)
      val keepAliveTime = conf.get(FRONTEND_THRIFT_WORKER_KEEPALIVE_TIME)
      val executor = new ThreadPoolExecutor(
        minThreads,
        maxThreads,
        keepAliveTime,
        TimeUnit.MILLISECONDS,
        new SynchronousQueue[Runnable](),
        new NamedThreadFactory(name + "Handler-Pool", false))
      val transFactory = authFactory.getTTransportFactory
      val tProcFactory = authFactory.getTProcessorFactory(this)
      val tServerSocket =
        // only enable ssl for server side
        if (isServer() && conf.get(FRONTEND_THRIFT_BINARY_SSL_ENABLED)) {
          keyStorePath = conf.get(FRONTEND_SSL_KEYSTORE_PATH)
          keyStorePassword = conf.get(FRONTEND_SSL_KEYSTORE_PASSWORD)
          keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE)
          val keyStoreAlgorithm = conf.get(FRONTEND_SSL_KEYSTORE_ALGORITHM)
          val disallowedSslProtocols = conf.get(FRONTEND_THRIFT_BINARY_SSL_DISALLOWED_PROTOCOLS)
          val includeCipherSuites = conf.get(FRONTEND_THRIFT_BINARY_SSL_INCLUDE_CIPHER_SUITES)

          if (keyStorePath.isEmpty) {
            throw new IllegalArgumentException(
              s"${FRONTEND_SSL_KEYSTORE_PATH.key} not configured for SSL connection")
          }
          if (keyStorePassword.isEmpty) {
            throw new IllegalArgumentException(
              s"${FRONTEND_SSL_KEYSTORE_PASSWORD.key} not configured for SSL connection")
          }

          getServerSSLSocket(
            keyStorePath.get,
            keyStorePassword.get,
            keyStoreType,
            keyStoreAlgorithm,
            disallowedSslProtocols,
            includeCipherSuites)
        } else {
          new TServerSocket(new ServerSocket(portNum, -1, serverAddr))
        }
      _actualPort = tServerSocket.getServerSocket.getLocalPort
      val maxMessageSize = conf.get(FRONTEND_THRIFT_MAX_MESSAGE_SIZE)
      val args = new TThreadPoolServer.Args(tServerSocket)
        .processorFactory(tProcFactory)
        .transportFactory(transFactory)
        .protocolFactory(new TBinaryProtocol.Factory)
        .inputProtocolFactory(
          new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
        // THRIFT-5297 (fixed in 0.14.0) removes requestTimeout and beBackoffSlotLength
        .executorService(executor)
      // TCP Server
      server = Some(new TThreadPoolServer(args))
      server.foreach(_.setServerEventHandler(new FeTServerEventHandler))
      info(s"Initializing $name on ${serverAddr.getHostName}:${_actualPort} with" +
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

  private def getServerSSLSocket(
      keyStorePath: String,
      keyStorePassword: String,
      keyStoreType: Option[String],
      keyStoreAlgorithm: Option[String],
      disallowedSslProtocols: Set[String],
      includeCipherSuites: Seq[String]): TServerSocket = {
    val params =
      if (includeCipherSuites.nonEmpty) {
        new TSSLTransportFactory.TSSLTransportParameters("TLS", includeCipherSuites.toArray)
      } else {
        new TSSLTransportFactory.TSSLTransportParameters()
      }
    params.setKeyStore(
      keyStorePath,
      keyStorePassword,
      keyStoreAlgorithm.getOrElse(KeyManagerFactory.getDefaultAlgorithm),
      keyStoreType.getOrElse(KeyStore.getDefaultType))

    val tServerSocket =
      TSSLTransportFactory.getServerSocket(portNum, 0, serverAddr, params)

    tServerSocket.getServerSocket match {
      case sslServerSocket: SSLServerSocket =>
        val lowerDisallowedSslProtocols = disallowedSslProtocols.map(_.toLowerCase(Locale.ROOT))
        val enabledProtocols = sslServerSocket.getEnabledProtocols.flatMap { protocol =>
          if (lowerDisallowedSslProtocols.contains(protocol.toLowerCase(Locale.ROOT))) {
            debug(s"Disabling SSL Protocol: $protocol")
            None
          } else {
            Some(protocol)
          }
        }
        sslServerSocket.setEnabledProtocols(enabledProtocols)
        info(s"SSL Server Socket enabled protocols: ${enabledProtocols.mkString(",")}")

      case _ =>
    }
    tServerSocket
  }

  override def run(): Unit =
    try {
      if (isServer()) {
        info(s"Starting and exposing JDBC connection at: jdbc:hive2://$connectionUrl/")
      }
      server.foreach(_.serve())
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
}
