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

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.{ChannelFuture, ChannelInitializer, ChannelOption}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.logging.{LoggingHandler, LogLevel}

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.util.ExecutorPoolCaptureOom
import org.apache.kyuubi.util.NettyUtils._

/**
 * A frontend service implement MySQL protocol.
 */
class KyuubiMySQLFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("MySQLFrontendService") with Logging {

  private var execPool: ThreadPoolExecutor = _

  private var serverAddr: InetAddress = _
  private var port: Int = _
  private var bootstrap: ServerBootstrap = _
  private var bindFuture: ChannelFuture = _

  @volatile protected var isStarted = false

  protected def oomHook: Runnable = () => serverable.stop()

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val minThreads = conf.get(FRONTEND_MYSQL_MIN_WORKER_THREADS)
    val maxThreads = conf.get(FRONTEND_MYSQL_MAX_WORKER_THREADS)
    val keepAliveMs = conf.get(FRONTEND_MYSQL_WORKER_KEEPALIVE_TIME)
    execPool = ExecutorPoolCaptureOom(
      "mysql-exec-pool",
      minThreads, maxThreads,
      keepAliveMs,
      oomHook)

    serverAddr = conf.get(FRONTEND_MYSQL_BIND_HOST)
      .map(InetAddress.getByName)
      .getOrElse(Utils.findLocalInetAddress)
    port = conf.get(FRONTEND_MYSQL_BIND_PORT)
    val workerThreads = defaultNumThreads(conf.get(FRONTEND_MYSQL_NETTY_WORKER_THREADS))
    val bossGroup = createEventLoop(1, "mysql-netty-boss")
    val workerGroup = createEventLoop(workerThreads, "mysql-netty-worker")
    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(SERVER_CHANNEL_CLASS)
      .option(ChannelOption.SO_BACKLOG, Int.box(128))
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .childOption(ChannelOption.TCP_NODELAY, Boolean.box(true))
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(channel: SocketChannel): Unit = channel.pipeline
          .addLast(new LoggingHandler("org.apache.kyuubi.server.mysql.codec", LogLevel.TRACE))
        // TODO implement authentication, codec, command handler
      })
    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    s"${serverAddr.getCanonicalHostName}:$port"
  }

  override def start(): Unit = synchronized {
    if (!isStarted) {
      try {
        bindFuture = bootstrap.bind(serverAddr, port)
        bindFuture.syncUninterruptibly
        port = bindFuture.channel.localAddress.asInstanceOf[InetSocketAddress].getPort
        isStarted = true
        info(s"MySQL frontend service has started at $connectionUrl.")
      } catch {
        case rethrow: Exception =>
          throw new KyuubiException("Cannot start MySQL frontend service Netty server", rethrow)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (isStarted) {
      if (bindFuture != null) {
        // close is a local operation and should finish within milliseconds; timeout just to be safe
        bindFuture.channel.close.awaitUninterruptibly(10, TimeUnit.SECONDS)
        bindFuture = null
      }
      if (bootstrap != null && bootstrap.config.group != null) {
        bootstrap.config.group.shutdownGracefully
      }
      if (bootstrap != null && bootstrap.config.childGroup != null) {
        bootstrap.config.childGroup.shutdownGracefully
      }
      bootstrap = null
      isStarted = false
    }
    super.stop()
  }

  override val discoveryService: Option[Service] = None
}
