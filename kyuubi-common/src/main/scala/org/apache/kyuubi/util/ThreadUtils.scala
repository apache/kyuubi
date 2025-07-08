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

import java.util.concurrent._

import scala.concurrent.Awaitable
import scala.concurrent.duration.{Duration, FiniteDuration}

import org.apache.kyuubi.{KyuubiException, Logging}

object ThreadUtils extends Logging {

  def newDaemonSingleThreadScheduledExecutor(
      threadName: String,
      executeExistingDelayedTasksAfterShutdown: Boolean = true): ScheduledExecutorService = {
    val threadFactory = new NamedThreadFactory(threadName, daemon = true)
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    executor.setRemoveOnCancelPolicy(true)
    executor
      .setExecuteExistingDelayedTasksAfterShutdownPolicy(executeExistingDelayedTasksAfterShutdown)
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
      poolSize,
      poolSize,
      keepAliveMs,
      TimeUnit.MILLISECONDS,
      queue,
      nameFactory)
    executor.allowCoreThreadTimeOut(true)
    executor
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = new NamedThreadFactory(prefix, daemon = true)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = new NamedThreadFactory(prefix, daemon = true)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonScheduledThreadPool(
      poolSize: Int,
      keepAliveSec: Long,
      prefix: String,
      removeOnCancel: Boolean = true,
      executeDelayedTasksAfterShutdown: Boolean = false): ScheduledThreadPoolExecutor = {
    val threadFactory = new NamedThreadFactory(prefix, daemon = true)
    val executor = new ScheduledThreadPoolExecutor(poolSize, threadFactory)
    executor.setRemoveOnCancelPolicy(removeOnCancel)
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(executeDelayedTasksAfterShutdown)
    if (keepAliveSec > 0) {
      executor.setKeepAliveTime(keepAliveSec, TimeUnit.SECONDS)
      executor.allowCoreThreadTimeOut(true)
    }
    info(s"$prefix: pool size: $poolSize, keepalive time: $keepAliveSec s")
    executor
  }

  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.result(atMost)(awaitPermission)
    } catch {
      case e: Exception =>
        throw new KyuubiException("Exception thrown in awaitResult: ", e)
    }
  }

  def shutdown(
      executor: ExecutorService,
      gracePeriod: Duration = FiniteDuration(30, TimeUnit.SECONDS)): Unit = {
    val shutdownTimeout = gracePeriod.toMillis
    if (executor != null) {
      executor.shutdown()
      try {
        executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)
      } catch {
        case e: InterruptedException =>
          warn(
            s"Exceeded timeout($shutdownTimeout ms) to wait the exec-pool shutdown gracefully",
            e)
      }
    }
  }

  def runInNewThread(
      threadName: String,
      isDaemon: Boolean = true)(body: => Unit): Unit = {

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        body
      }
    }
    thread.setDaemon(isDaemon)
    thread.setUncaughtExceptionHandler(NamedThreadFactory.kyuubiUncaughtExceptionHandler)
    thread.start()
  }

  /**
   * Schedule a runnable to the scheduled executor service.
   * The exceptions thrown in the runnable will be caught and logged.
   */
  def scheduleTolerableRunnableWithFixedDelay(
      scheduler: ScheduledExecutorService,
      runnable: Runnable,
      initialDelay: Long,
      delay: Long,
      timeUnit: TimeUnit): Unit = {
    scheduler.scheduleWithFixedDelay(
      () =>
        try {
          runnable.run()
        } catch {
          case t: Throwable =>
            error(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        },
      initialDelay,
      delay,
      timeUnit)
  }
}
