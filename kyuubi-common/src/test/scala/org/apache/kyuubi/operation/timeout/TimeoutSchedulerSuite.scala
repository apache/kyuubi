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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.NoopOperationManager

class TimeoutSchedulerSuite extends KyuubiFunSuite {

  test("scheduler lifecycle and functionality via OperationManager") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME, 30000L)

    val manager = new NoopOperationManager()
    manager.initialize(conf)
    manager.start()

    val latch = new CountDownLatch(1)
    val future = manager.scheduleTimeout(
      new Runnable {
        override def run(): Unit = latch.countDown()
      },
      1)

    // Ensure the task completed
    assert(latch.await(5, TimeUnit.SECONDS))
    assert(!future.isCancelled)

    // Test cancellation path
    val longFuture = manager.scheduleTimeout(new Runnable { override def run(): Unit = {} }, 10)
    manager.cancelTimeout(longFuture)
    assert(longFuture.isCancelled)

    manager.stop()
  }
}
