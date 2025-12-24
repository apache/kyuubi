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

import java.util.concurrent.TimeUnit

import org.apache.kyuubi.KyuubiFunSuite

class ThreadUtilsSuite extends KyuubiFunSuite {

  test("New daemon single thread scheduled executor for shutdown") {
    val service = ThreadUtils.newDaemonSingleThreadScheduledExecutor("ThreadUtilsTest")
    @volatile var threadName = ""
    service.submit(new Runnable {
      override def run(): Unit = {
        threadName = Thread.currentThread().getName
      }
    })
    service.shutdown()
    service.awaitTermination(10, TimeUnit.SECONDS)
    assert(threadName startsWith "ThreadUtilsTest")
  }

  test("New daemon single thread scheduled executor for shutdownNow") {
    val service = ThreadUtils.newDaemonSingleThreadScheduledExecutor("ThreadUtilsTest")
    @volatile var threadName = ""
    service.submit(new Runnable {
      override def run(): Unit = {
        threadName = Thread.currentThread().getName
      }
    })
    service.shutdownNow()
    service.awaitTermination(10, TimeUnit.SECONDS)
    assert(threadName startsWith "")
  }

  test("New daemon single thread scheduled executor for cancel delayed tasks") {
    val service = ThreadUtils.newDaemonSingleThreadScheduledExecutor("ThreadUtilsTest", false)
    @volatile var threadName = ""
    service.submit(new Runnable {
      override def run(): Unit = {
        threadName = Thread.currentThread().getName
      }
    })
    service.shutdown()
    service.awaitTermination(10, TimeUnit.SECONDS)
    assert(threadName startsWith "")
  }

  test("New daemon scheduled thread pool") {
    val pool = ThreadUtils.newDaemonScheduledThreadPool(2, 10, "ThreadUtilsSchedTest")
    // submit a task to ensure pool operational
    @volatile var ran = false
    val fut = pool.schedule(
      new Runnable { override def run(): Unit = ran = true },
      100,
      TimeUnit.MILLISECONDS)
    fut.get(5, TimeUnit.SECONDS)
    assert(ran)
    assert(pool.getCorePoolSize == 2)
    ThreadUtils.shutdown(pool)
    assert(pool.isShutdown)
  }
}
