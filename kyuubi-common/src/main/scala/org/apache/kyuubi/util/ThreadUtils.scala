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

import java.util.concurrent.{Executors, ExecutorService, LinkedBlockingQueue, ScheduledExecutorService, ScheduledThreadPoolExecutor, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.Awaitable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal

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

  def runInNewThread[T](
      threadName: String,
      isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()

    exception match {
      case Some(realException) =>
        // Remove the part of the stack that shows method calls into this helper method
        // This means drop everything from the top until the stack element
        // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
        val baseStackTrace = Thread.currentThread().getStackTrace().dropWhile(
          !_.getClassName.contains(this.getClass.getSimpleName)).drop(1)

        // Remove the part of the new thread stack that shows methods call from this helper method
        val extraStackTrace = realException.getStackTrace.takeWhile(
          !_.getClassName.contains(this.getClass.getSimpleName))

        // Combine the two stack traces, with a place holder just specifying that there
        // was a helper method used, without any further details of the helper
        val placeHolderStackElem = new StackTraceElement(
          s"... run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")} ..",
          " ",
          "",
          -1)
        val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

        // Update the stack trace and rethrow the exception in the caller thread
        realException.setStackTrace(finalStackTrace)
        throw realException
      case None =>
        result
    }
  }
}
