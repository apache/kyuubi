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

import java.util.concurrent.{Future, SynchronousQueue, ThreadPoolExecutor, TimeUnit}

case class ExecutorPoolCaptureOom(
    poolName: String,
    corePoolSize: Int,
    maximumPoolSize: Int,
    keepAliveSeconds: Long,
    hook: Runnable)
  extends ThreadPoolExecutor(
    corePoolSize,
    maximumPoolSize,
    keepAliveSeconds,
    TimeUnit.MILLISECONDS,
    new SynchronousQueue[Runnable](),
    new NamedThreadFactory(poolName, false)) {

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    super.afterExecute(r, t)
    t match {
      case _: OutOfMemoryError => hook.run()
      case null => r match {
        case f: Future[_] => try {
          if (f.isDone) f.get()
        } catch {
          case _: InterruptedException => Thread.currentThread().interrupt()
          case _: OutOfMemoryError => hook.run()
        }
        case _ =>
      }
      case _ =>
    }
  }
}
