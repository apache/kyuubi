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

import java.util.concurrent.{CountDownLatch, RejectedExecutionException, TimeUnit}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.util.ThreadUtils

class VirtualThreadPoolSuite extends KyuubiFunSuite {

  test("Jetty virtual thread pool lifecycle and concurrency limit") {
    if (ThreadUtils.isVirtualThreadSupported) {
      val pool = new VirtualThreadPool(2, "VirtualThreadPoolTest")
      val ready = new CountDownLatch(2)
      val release = new CountDownLatch(1)
      val isVirtual = classOf[Thread].getMethod("isVirtual")

      val blockingTask = new Runnable {
        override def run(): Unit = {
          assert(isVirtual.invoke(Thread.currentThread()).asInstanceOf[Boolean])
          ready.countDown()
          release.await()
        }
      }

      pool.start()
      try {
        assert(pool.getIdleThreads === 0)
        pool.execute(blockingTask)
        pool.execute(blockingTask)
        assert(ready.await(10, TimeUnit.SECONDS))
        assert(pool.getThreads === 2)
        assert(pool.isLowOnThreads)
        intercept[RejectedExecutionException](pool.execute(blockingTask))
      } finally {
        release.countDown()
        pool.stop()
      }
      intercept[RejectedExecutionException](pool.execute(() => ()))

      pool.start()
      try {
        val restarted = new CountDownLatch(1)
        pool.execute(() => restarted.countDown())
        assert(restarted.await(10, TimeUnit.SECONDS))
      } finally {
        pool.stop()
      }
    }
  }
}
