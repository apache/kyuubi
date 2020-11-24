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

import java.util.concurrent.{LinkedBlockingQueue, ScheduledExecutorService, ScheduledThreadPoolExecutor, ThreadPoolExecutor, TimeUnit}

import org.apache.kyuubi.Logging

object ThreadUtils extends Logging {

  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new NamedThreadFactory(threadName, daemon = true)
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  def newDaemonQueuedThreadPool(
      poolSize: Int,
      poolQueueSize: Int,
      keepAliveMs: Long,
      threadPoolName: String): ThreadPoolExecutor = {
    val nameFactory = new NamedThreadFactory(threadPoolName, daemon = true)
    val queue = new LinkedBlockingQueue[Runnable](poolQueueSize)
    info(s"$threadPoolName: pool size: $poolSize, wait queue size: $poolQueueSize," +
      s" thread keepalive time: $keepAliveMs ms")
    val executor = new ThreadPoolExecutor(
      poolSize, poolSize, keepAliveMs, TimeUnit.MILLISECONDS, queue, nameFactory)
    executor.allowCoreThreadTimeOut(true)
    executor
  }
}
