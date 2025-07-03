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

package org.apache.kyuubi.operation

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.util.{NamedThreadFactory, ThreadUtils}

/**
 * Thread pool based timeout executor that shares a pool of threads
 * for managing operation timeouts.
 *
 * @param poolSize the number of threads in the pool
 * @param keepAliveSeconds the keep-alive time for idle threads
 */
class ThreadPoolTimeoutExecutor(
    poolSize: Int,
    keepAliveSeconds: Long) extends Logging {

  private val executor: ScheduledExecutorService = {
    val threadFactory = new NamedThreadFactory("operation-timeout-pool", daemon = true)
    val scheduledExecutor = new ScheduledThreadPoolExecutor(poolSize, threadFactory)
    scheduledExecutor.setRemoveOnCancelPolicy(true)
    scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    // Apply keepAlive if configured (>0). Enable core thread timeout accordingly.
    if (keepAliveSeconds > 0) {
      scheduledExecutor.setKeepAliveTime(keepAliveSeconds, TimeUnit.SECONDS)
      scheduledExecutor.allowCoreThreadTimeOut(true)
    }
    info(s"operation-timeout-pool: pool size: $poolSize, keepalive time: $keepAliveSeconds s")
    scheduledExecutor
  }

  def scheduleTimeout(
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

  def cancelTimeout(future: ScheduledFuture[_]): Unit = {
    if (future != null && !future.isCancelled) {
      future.cancel(false)
    }
  }

  def shutdown(): Unit = {
    ThreadUtils.shutdown(executor)
  }

  def isShutdown: Boolean = {
    executor.isShutdown
  }
}

// Companion object to manage a singleton instance across the server lifecycle
object ThreadPoolTimeoutExecutor extends Logging {

  import com.google.common.annotations.VisibleForTesting

  import org.apache.kyuubi.config.KyuubiConf

  @volatile private var _instance: Option[ThreadPoolTimeoutExecutor] = None

  /**
   * Return the global timeout executor, creating it if necessary (thread-safe).
   */
  def getOrCreate(conf: KyuubiConf): ThreadPoolTimeoutExecutor = {
    _instance.getOrElse {
      synchronized {
        _instance.getOrElse {
          val poolSize = conf.get(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE)
          val keepAlive = conf.get(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME)
          val inst = new ThreadPoolTimeoutExecutor(poolSize, keepAlive)
          _instance = Some(inst)
          inst
        }
      }
    }
  }

  /** Gracefully shut down the shared executor if it exists. */
  def shutdown(): Unit = {
    _instance.foreach { inst =>
      if (!inst.isShutdown) inst.shutdown()
    }
    _instance = None
  }

  /** Reset singleton for unit tests. */
  @VisibleForTesting
  private[kyuubi] def reset(): Unit = synchronized {
    _instance.foreach(_.shutdown())
    _instance = None
  }
}
