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

import java.util.concurrent.ScheduledFuture

/**
 * Interface for managing operation timeouts.
 * This allows different implementations for timeout management strategies.
 */
trait TimeoutExecutor {

  /**
   * Schedule a timeout for an operation.
   *
   * @param timeoutAction the action to execute when timeout occurs
   * @param timeoutSeconds the timeout duration in seconds
   * @return a ScheduledFuture that can be used to cancel the timeout
   */
  def scheduleTimeout(timeoutAction: Runnable, timeoutSeconds: Long): ScheduledFuture[_]

  /**
   * Cancel a scheduled timeout.
   *
   * @param future the ScheduledFuture returned by scheduleTimeout
   */
  def cancelTimeout(future: ScheduledFuture[_]): Unit

  /**
   * Shutdown the executor and release resources.
   */
  def shutdown(): Unit

  /**
   * Check if the executor is shutdown.
   *
   * @return true if shutdown, false otherwise
   */
  def isShutdown: Boolean
}
