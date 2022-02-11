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

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.{TServer, TThreadPoolServer}
import org.apache.thrift.transport.TServerSocket

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
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
  final override protected lazy val portNum: Int = conf.get(FRONTEND_THRIFT_BINARY_BIND_PORT)

  private var server: Option[TServer] = None

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
      val tServerSocket = new TServerSocket(serverSocket)
      val maxMessageSize = conf.get(FRONTEND_THRIFT_MAX_MESSAGE_SIZE)
      val requestTimeout = conf.get(FRONTEND_THRIFT_LOGIN_TIMEOUT).toInt
      val beBackoffSlotLength = conf.get(FRONTEND_THRIFT_LOGIN_BACKOFF_SLOT_LENGTH).toInt
      val args = new TThreadPoolServer.Args(tServerSocket)
        .processorFactory(tProcFactory)
        .transportFactory(transFactory)
        .protocolFactory(new TBinaryProtocol.Factory)
        .inputProtocolFactory(
          new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
        .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.MILLISECONDS)
        .beBackoffSlotLength(beBackoffSlotLength)
        .beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
        .executorService(executor)
      // TCP Server
      server = Some(new TThreadPoolServer(args))
      server.foreach(_.setServerEventHandler(new FeTServerEventHandler))
      info(s"Initializing $name on ${serverAddr.getHostName}:${serverSocket.getLocalPort} with" +
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
