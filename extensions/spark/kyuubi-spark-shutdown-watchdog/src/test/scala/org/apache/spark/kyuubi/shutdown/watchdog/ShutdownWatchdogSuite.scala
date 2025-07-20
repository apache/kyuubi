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

package org.apache.spark.kyuubi.shutdown.watchdog

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntConsumer

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Seconds, Span}
import org.slf4j.helpers.NOPLogger

class ShutdownWatchdogSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with Eventually {

  private val logger = NOPLogger.NOP_LOGGER

  override protected def afterEach(): Unit = {
    ShutdownWatchdog.resetForTests()
    super.afterEach()
  }

  test("watchdog does not start when disabled") {
    val conf = new SparkConf(false)
      .set(SparkShutdownWatchdogConf.SHUTDOWN_WATCHDOG_ENABLED_KEY, "false")
      .set(SparkShutdownWatchdogConf.SHUTDOWN_WATCHDOG_TIMEOUT_KEY, "1000ms")

    ShutdownWatchdog.startIfNeeded(conf, logger)
    assert(!ShutdownWatchdog.isRunningForTests())
  }

  test("watchdog does not start when timeout is non-positive") {
    val conf = new SparkConf(false)
      .set(SparkShutdownWatchdogConf.SHUTDOWN_WATCHDOG_ENABLED_KEY, "true")
      .set(SparkShutdownWatchdogConf.SHUTDOWN_WATCHDOG_TIMEOUT_KEY, "0ms")

    ShutdownWatchdog.startIfNeeded(conf, logger)
    assert(!ShutdownWatchdog.isRunningForTests())
  }

  test("watchdog triggers emergency exit after timeout") {
    val conf = new SparkConf(false)
      .set(SparkShutdownWatchdogConf.SHUTDOWN_WATCHDOG_ENABLED_KEY, "true")
      .set(SparkShutdownWatchdogConf.SHUTDOWN_WATCHDOG_TIMEOUT_KEY, "25ms")

    val exitCode = new AtomicInteger(-1)
    val exitLatch = new CountDownLatch(1)

    ShutdownWatchdog.setExitFn(new IntConsumer {
      override def accept(value: Int): Unit = {
        exitCode.set(value)
        exitLatch.countDown()
      }
    })

    ShutdownWatchdog.startIfNeeded(conf, logger)

    assert(exitLatch.await(5, TimeUnit.SECONDS), "Watchdog did not trigger within timeout")
    assert(exitCode.get() == ShutdownWatchdog.EXIT_CODE_FORCED_TERMINATION)

    // Ensure the watchdog thread cleaned itself up.
    eventually(Timeout(Span(2, Seconds))) {
      assert(!ShutdownWatchdog.isRunningForTests())
    }
  }
}
