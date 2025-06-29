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

class TimeoutExecutorFactorySuite extends KyuubiFunSuite {

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Ensure clean state before each test
    TimeoutExecutorFactory.reset()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Clean up after each test
    TimeoutExecutorFactory.reset()
  }

  test("get per-operation timeout executor by default") {
    val conf = new KyuubiConf()
    val executor = TimeoutExecutorFactory.getExecutor(conf)
    assert(executor.isInstanceOf[PerOperationTimeoutExecutor])
    // PerOperationTimeoutExecutor is not shutdown by default
  }

  test("get per-operation timeout executor explicitly") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "per-operation")
    val executor = TimeoutExecutorFactory.getExecutor(conf)
    assert(executor.isInstanceOf[PerOperationTimeoutExecutor])
    // PerOperationTimeoutExecutor is not shutdown by default
  }

  test("get thread-pool timeout executor") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "thread-pool")
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME, 30000L)

    val executor = TimeoutExecutorFactory.getExecutor(conf)
    assert(executor.isInstanceOf[ThreadPoolTimeoutExecutor])
    assert(!executor.isShutdown)
  }

  test("thread-pool executor singleton behavior") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "thread-pool")
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)

    val executor1 = TimeoutExecutorFactory.getExecutor(conf)
    val executor2 = TimeoutExecutorFactory.getExecutor(conf)

    // Should return the same instance for thread-pool type
    assert(executor1 eq executor2)
    assert(executor1.isInstanceOf[ThreadPoolTimeoutExecutor])
  }

  test("per-operation executor creates new instances") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "per-operation")

    val executor1 = TimeoutExecutorFactory.getExecutor(conf)
    val executor2 = TimeoutExecutorFactory.getExecutor(conf)

    // Should return different instances for per-operation type
    assert(executor1 ne executor2)
    assert(executor1.isInstanceOf[PerOperationTimeoutExecutor])
    assert(executor2.isInstanceOf[PerOperationTimeoutExecutor])
  }

  test("invalid executor type throws exception") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "unknown-type")

    // Should throw IllegalArgumentException for invalid executor type
    intercept[IllegalArgumentException] {
      TimeoutExecutorFactory.getExecutor(conf)
    }
  }

  test("timeout executor factory shutdown - thread-pool type") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "thread-pool")
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)

    // Get the thread-pool executor to initialize it
    val executor = TimeoutExecutorFactory.getExecutor(conf)
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
    TimeoutExecutorFactory.shutdown()

    // Verify the executor is shutdown
    assert(
      executor.isShutdown,
      "ThreadPoolTimeoutExecutor should be shutdown after factory shutdown")
  }

  test("timeout executor factory shutdown - per-operation type") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "per-operation")

    // Get a per-operation executor
    val executor = TimeoutExecutorFactory.getExecutor(conf)
    assert(executor.isInstanceOf[PerOperationTimeoutExecutor])

    // For per-operation type, factory shutdown should be safe (no-op)
    TimeoutExecutorFactory.shutdown()

    // Per-operation executors are independent, so factory shutdown doesn't affect them
    // (they manage their own lifecycle)
  }

  test("timeout executor factory shutdown - multiple calls are safe") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "thread-pool")
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 2)

    // Initialize the thread-pool executor
    val executor = TimeoutExecutorFactory.getExecutor(conf)
    assert(!executor.isShutdown)

    // Multiple shutdown calls should be safe
    TimeoutExecutorFactory.shutdown()
    assert(executor.isShutdown)

    // Additional shutdown calls should not cause issues
    TimeoutExecutorFactory.shutdown()
    TimeoutExecutorFactory.shutdown()

    assert(executor.isShutdown)
  }

  test("timeout executor factory shutdown - no executor initialized") {
    // Calling shutdown when no executor has been initialized should be safe
    TimeoutExecutorFactory.shutdown()
    // No assertion needed, just verify no exception is thrown
  }

  test("thread-pool executor functionality after factory creation") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.OPERATION_TIMEOUT_EXECUTOR_TYPE, "thread-pool")
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_SIZE, 3)
    conf.set(KyuubiConf.OPERATION_TIMEOUT_POOL_KEEPALIVE_TIME, 60000L)

    val executor = TimeoutExecutorFactory.getExecutor(conf)

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
