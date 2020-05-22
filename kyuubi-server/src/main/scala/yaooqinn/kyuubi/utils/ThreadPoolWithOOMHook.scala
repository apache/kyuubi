/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.utils

import java.util.concurrent.{BlockingQueue, Future, ThreadFactory, ThreadPoolExecutor}

import scala.concurrent.duration.TimeUnit

class ThreadPoolWithOOMHook(
    corePoolSize: Int,
    maximumPoolSize: Int,
    keepAliveTime: Long,
    unit: TimeUnit,
    workQueue: BlockingQueue[Runnable],
    threadFactory: ThreadFactory,
    oomHook: Runnable)
  extends ThreadPoolExecutor(
    corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory) {

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    super.afterExecute(r, t)
    var throwable = t
    if (throwable == null && r.isInstanceOf[Future[_]]) {
      try {
        val future = r.asInstanceOf[Future[_]]
        if (future.isDone) {
          future.get()
        }
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
        case t2: Throwable =>
          throwable = t2
      }
    }
    if (throwable.isInstanceOf[OutOfMemoryError]) oomHook.run()
  }
}
