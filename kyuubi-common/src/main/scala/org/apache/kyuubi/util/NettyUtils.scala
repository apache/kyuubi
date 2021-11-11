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

package org.apache.kyuubi.util

import java.util.concurrent.ThreadFactory

import scala.math.min

import io.netty.channel._
import io.netty.channel.epoll._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.util.concurrent.DefaultThreadFactory

object NettyUtils {

  /**
   * Specifies an upper bound on the number of Netty threads that Kyuubi requires by default.
   * In practice, only 2-4 cores should be required to transfer roughly 10 Gb/s, and each core
   * that we use will have an initial overhead of roughly 32 MB of off-heap memory, which comes
   * at a premium.
   *
   * Thus, this value should still retain maximum throughput and reduce wasted off-heap memory
   * allocation.
   */
  val MAX_NETTY_THREADS: Int = 8

  val EPOLL_MODE: Boolean = Epoll.isAvailable

  val CLIENT_CHANNEL_CLASS: Class[_ <: Channel] =
    if (EPOLL_MODE) classOf[EpollSocketChannel] else classOf[NioSocketChannel]

  val SERVER_CHANNEL_CLASS: Class[_ <: ServerChannel] =
    if (EPOLL_MODE) classOf[EpollServerSocketChannel] else classOf[NioServerSocketChannel]

  def createThreadFactory(threadPoolPrefix: String): ThreadFactory =
    new DefaultThreadFactory(threadPoolPrefix, true)

  def createEventLoop(numThreads: Int, threadPrefix: String): EventLoopGroup = {
    val threadFactory = createThreadFactory(threadPrefix)
    if (EPOLL_MODE) {
      new EpollEventLoopGroup(numThreads, threadFactory)
    } else {
      new NioEventLoopGroup(numThreads, threadFactory)
    }
  }

  /**
   * Returns the default number of threads for the Netty thread pools. If numUsableCores is absent,
   * we will use Runtime get an approximate number of available cores.
   */
  def defaultNumThreads(numUsableCores: Option[Int]): Int = numUsableCores match {
    case Some(num) => min(num, MAX_NETTY_THREADS)
    case None => min(sys.runtime.availableProcessors, MAX_NETTY_THREADS)
  }
}
