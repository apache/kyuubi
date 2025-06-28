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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

/**
 * Factory for creating timeout executors based on configuration.
 */
object TimeoutExecutorFactory extends Logging {

  private var threadPoolExecutor: ThreadPoolTimeoutExecutor = _

  /**
   * Create a timeout executor based on configuration.
   *
   * @param conf the Kyuubi configuration
   * @return a TimeoutExecutor instance
   */
  def getExecutor(conf: KyuubiConf): TimeoutExecutor = {
    val executorTypeStr = conf.get(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE)
    executorTypeStr.toLowerCase match {
      case "thread-pool" =>
        if (threadPoolExecutor == null) {
          synchronized {
            if (threadPoolExecutor == null) {
              val poolSize = conf.get(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE)
              val keepAliveMs = conf.get(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME)
              info(s"Creating thread pool timeout executor: poolSize=$poolSize, " +
                s"keepAliveMs=$keepAliveMs")
              threadPoolExecutor = new ThreadPoolTimeoutExecutor(poolSize, keepAliveMs)
            }
          }
        }
        threadPoolExecutor
      case "per-operation" =>
        new PerOperationTimeoutExecutor
      case _ =>
        warn(s"Unknown executor type: $executorTypeStr, falling back to per-operation")
        new PerOperationTimeoutExecutor
    }
  }

  def shutdown(): Unit = {
    if (threadPoolExecutor != null && !threadPoolExecutor.isShutdown) {
      threadPoolExecutor.shutdown()
    }
  }
}
