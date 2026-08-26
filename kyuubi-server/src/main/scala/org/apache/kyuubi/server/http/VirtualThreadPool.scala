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

package org.apache.kyuubi.server.http

import java.util.concurrent.{ExecutorService, RejectedExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.eclipse.jetty.util.component.AbstractLifeCycle
import org.eclipse.jetty.util.thread.ThreadPool

import org.apache.kyuubi.util.ThreadUtils

/**
 * Adapts a virtual-thread-per-task executor to Jetty's [[ThreadPool]] contract.
 *
 * Despite the name, virtual threads are never pooled or reused. Each task runs on a new virtual
 * thread, while `maxThreads` only limits concurrent tasks to preserve frontend backpressure.
 *
 * TODO: When Kyuubi upgrades to Jetty 12, replace this adapter with Jetty's native
 * `org.eclipse.jetty.util.thread.VirtualThreadPool`.
 */
private[server] class VirtualThreadPool(maxThreads: Int, name: String)
  extends AbstractLifeCycle with ThreadPool.SizedThreadPool {

  private val activeThreads = new AtomicInteger()
  private val executor: ExecutorService =
    ThreadUtils.newBoundedVirtualThreadPerTaskExecutor(maxThreads, name)

  override def execute(command: Runnable): Unit = {
    if (!isRunning) {
      throw new RejectedExecutionException(s"$name is not running")
    }
    executor.execute(() => {
      activeThreads.incrementAndGet()
      try {
        command.run()
      } finally {
        activeThreads.decrementAndGet()
      }
    })
  }

  override def join(): Unit = executor.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)

  override def getThreads: Int = activeThreads.get()

  override def getIdleThreads: Int = Math.max(maxThreads - activeThreads.get(), 0)

  override def isLowOnThreads: Boolean = activeThreads.get() >= maxThreads

  override def getMinThreads: Int = 0

  override def getMaxThreads: Int = maxThreads

  override def setMinThreads(threads: Int): Unit = {
    require(threads == 0, "The virtual thread pool does not retain idle workers")
  }

  override def setMaxThreads(threads: Int): Unit = {
    require(threads == maxThreads, "The virtual thread pool maximum is immutable")
  }

  override protected def doStop(): Unit = {
    executor.shutdownNow()
    executor.awaitTermination(30, TimeUnit.SECONDS)
  }
}
