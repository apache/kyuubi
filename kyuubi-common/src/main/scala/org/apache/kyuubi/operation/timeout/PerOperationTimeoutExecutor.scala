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

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.util.ThreadUtils

/**
 * Per-operation timeout executor that creates a dedicated thread
 * for each operation timeout (current implementation).
 */
class PerOperationTimeoutExecutor extends TimeoutExecutor with Logging {

  private var executor: Option[ScheduledExecutorService] = None
  private var scheduledFuture: Option[ScheduledFuture[_]] = None

  override def scheduleTimeout(
      timeoutAction: Runnable,
      timeoutSeconds: Long): ScheduledFuture[_] = {
    // Create a new executor for this operation
    val timeoutExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("query-timeout-thread", false)
    executor = Some(timeoutExecutor)

    val wrappedAction: Runnable = () => {
      try {
        timeoutAction.run()
      } catch {
        case e: Exception =>
          error("Error during timeout action execution", e)
      }
    }

    val future = timeoutExecutor.schedule(wrappedAction, timeoutSeconds, TimeUnit.SECONDS)
    scheduledFuture = Some(future)
    future
  }

  override def cancelTimeout(future: ScheduledFuture[_]): Unit = {
    scheduledFuture.foreach { f =>
      if (!f.isCancelled) {
        f.cancel(false)
      }
    }
    shutdown()
  }

  override def shutdown(): Unit = {
    executor.foreach(_.shutdown())
    executor = None
    scheduledFuture = None
  }

  override def isShutdown: Boolean = {
    executor.forall(_.isShutdown)
  }
}
