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

package org.apache.kyuubi.operation.timeout

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.util.{NamedThreadFactory, ThreadUtils}

/**
 * Thread pool based timeout executor that shares a pool of threads
 * for managing operation timeouts.
 *
 * @param poolSize the number of threads in the pool
 * @param keepAliveMs the keep-alive time for idle threads
 */
class ThreadPoolTimeoutExecutor(
    poolSize: Int,
    keepAliveMs: Long) extends TimeoutExecutor with Logging {

  private val executor: ScheduledExecutorService = {
    val threadFactory = new NamedThreadFactory("operation-timeout-pool", daemon = true)
    val scheduledExecutor = new ScheduledThreadPoolExecutor(poolSize, threadFactory)
    scheduledExecutor.setRemoveOnCancelPolicy(true)
    scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    info(s"operation-timeout-pool: pool size: $poolSize, keepalive time: $keepAliveMs ms")
    scheduledExecutor
  }

  override def scheduleTimeout(
      timeoutAction: Runnable,
      timeoutSeconds: Long): ScheduledFuture[_] = {
    val wrappedAction: Runnable = () => {
      try {
        timeoutAction.run()
      } catch {
        case e: Exception =>
          error("Error during timeout action execution", e)
      }
    }

    executor.schedule(wrappedAction, timeoutSeconds, TimeUnit.SECONDS)
  }

  override def cancelTimeout(future: ScheduledFuture[_]): Unit = {
    if (future != null && !future.isCancelled) {
      future.cancel(false)
    }
  }

  override def shutdown(): Unit = {
    ThreadUtils.shutdown(executor)
  }

  override def isShutdown: Boolean = {
    executor.isShutdown
  }
}
