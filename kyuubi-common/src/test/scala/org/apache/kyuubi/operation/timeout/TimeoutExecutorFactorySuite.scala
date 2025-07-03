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
import org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor

class ThreadPoolTimeoutExecutorSuite extends KyuubiFunSuite {

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Ensure clean state before each test
    org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.reset()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Clean up after each test
    org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.reset()
  }

  test("thread-pool timeout executor is default and functional") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME, 30000L)

    val executor = org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.getOrCreate(conf)
    assert(executor.isInstanceOf[ThreadPoolTimeoutExecutor])
    assert(!executor.isShutdown)
  }

  test("thread-pool executor singleton behavior") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)

    val executor1 = org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.getOrCreate(conf)
    val executor2 = org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.getOrCreate(conf)

    // Should return the same instance for thread-pool type
    assert(executor1 eq executor2)
    assert(executor1.isInstanceOf[ThreadPoolTimeoutExecutor])
  }

  test("timeout executor factory shutdown works") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)

    // Get the thread-pool executor to initialize it
    val executor = org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.getOrCreate(conf)
    assert(executor.isInstanceOf[ThreadPoolTimeoutExecutor])
    assert(!executor.isShutdown)

    // Schedule a timeout action to verify the executor is working
    val latch = new CountDownLatch(1)
    val timeoutAction = new Runnable {
      override def run(): Unit = latch.countDown()
    }

    val future = executor.scheduleTimeout(timeoutAction, 1)

    // Wait for the timeout action to execute
    assert(latch.await(5, TimeUnit.SECONDS), "Timeout action should have executed")

    // Now shutdown the factory
    org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.shutdown()

    // Verify the executor is shutdown
    assert(
      executor.isShutdown,
      "ThreadPoolTimeoutExecutor should be shutdown after factory shutdown")
  }

  test("timeout executor factory shutdown - multiple calls are safe") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)

    // Initialize the thread-pool executor
    val executor = org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.getOrCreate(conf)
    assert(!executor.isShutdown)

    // Multiple shutdown calls should be safe
    org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.shutdown()
    assert(executor.isShutdown)

    // Additional shutdown calls should not cause issues
    org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.shutdown()
    org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.shutdown()

    assert(executor.isShutdown)
  }

  test("timeout executor factory shutdown - no executor initialized") {
    // Calling shutdown when no executor has been initialized should be safe
    org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.shutdown()
    // No assertion needed, just verify no exception is thrown
  }

  test("thread-pool executor functionality after factory creation") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 3)
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME, 60000L)

    val executor = org.apache.kyuubi.operation.ThreadPoolTimeoutExecutor.getOrCreate(conf)

    // Test scheduling multiple timeout actions
    val latch = new CountDownLatch(2)
    val timeoutAction = new Runnable {
      override def run(): Unit = latch.countDown()
    }

    val future1 = executor.scheduleTimeout(timeoutAction, 1)
    val future2 = executor.scheduleTimeout(timeoutAction, 1)

    // Wait for both actions to execute
    assert(latch.await(5, TimeUnit.SECONDS), "Both timeout actions should have executed")

    // Test cancellation (before shutdown)
    val future3 = executor.scheduleTimeout(timeoutAction, 10) // Long timeout
    executor.cancelTimeout(future3)
    assert(future3.isCancelled, "Future should be cancelled")

    // The factory shutdown will be called in afterEach
  }
}
