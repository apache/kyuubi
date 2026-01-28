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

package org.apache.spark.kyuubi.shutdown.watchdog;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.util.SparkExitCode;
import org.slf4j.Logger;

/**
 * Timer-backed watchdog that forces the Spark driver to exit if a graceful shutdown stalls.
 *
 * <p>Implementation is deliberately pure Java so the plugin has no Scala runtime dependency.
 */
public final class ShutdownWatchdog {

  // Align with Spark's standard exit codes for consistency with Spark driver failures.
  static final int EXIT_CODE_FORCED_TERMINATION = SparkExitCode.UNCAUGHT_EXCEPTION();
  static final int EXIT_CODE_WATCHDOG_FAILURE = SparkExitCode.UNCAUGHT_EXCEPTION_TWICE();

  private static final AtomicReference<Thread> WATCHDOG_THREAD_REF = new AtomicReference<>();
  private static volatile IntConsumer exitFn = System::exit;

  private ShutdownWatchdog() {}

  static void setExitFn(IntConsumer fn) {
    exitFn = fn != null ? fn : System::exit;
  }

  static void startIfNeeded(SparkConf sparkConf, Logger logger) {
    Objects.requireNonNull(sparkConf, "sparkConf");
    Objects.requireNonNull(logger, "logger");

    if (!SparkShutdownWatchdogConf.isEnabled(sparkConf)) {
      logger.info(
          "Shutdown Watchdog is disabled via {}=false.",
          SparkShutdownWatchdogConf.SHUTDOWN_WATCHDOG_ENABLED_KEY);
      return;
    }

    final long timeoutMillis = SparkShutdownWatchdogConf.getTimeoutMillis(sparkConf);
    if (timeoutMillis <= 0L) {
      logger.info("Shutdown Watchdog is disabled because timeout <= 0.");
      return;
    }

    Thread existing = WATCHDOG_THREAD_REF.get();
    if (existing != null && existing.isAlive()) {
      logger.warn("Shutdown Watchdog is already running, ignoring duplicate start request.");
      return;
    }

    final Thread watchdogThread =
        new Thread(() -> runWatchdogLoop(timeoutMillis, logger), "shutdown-watchdog");
    watchdogThread.setDaemon(true);

    if (!WATCHDOG_THREAD_REF.compareAndSet(existing, watchdogThread)) {
      logger.warn("Shutdown Watchdog could not be started because another instance won the race.");
      return;
    }

    logger.info(
        "Shutdown Watchdog activated. Driver will be forcefully terminated if graceful shutdown "
            + "exceeds {} ms.",
        timeoutMillis);

    watchdogThread.start();

    if (logger.isDebugEnabled()) {
      logger.debug("Shutdown Watchdog thread started: {}", watchdogThread.getName());
    }
  }

  private static void runWatchdogLoop(long timeoutMillis, Logger logger) {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Shutdown Watchdog thread monitoring with timeout {} ms.", timeoutMillis);
      }

      TimeUnit.MILLISECONDS.sleep(timeoutMillis);

      logger.error("EMERGENCY SHUTDOWN TRIGGERED");
      logger.error("Graceful shutdown exceeded {} ms timeout", timeoutMillis);
      logger.error("Non-daemon threads are preventing JVM exit");
      logger.error("Initiating forced termination...");

      logger.error("=== THREAD DUMP FOR DIAGNOSTIC ===");
      ThreadDumpUtils.dumpToLogger(logger);
      logger.error("=== END OF THREAD DUMP ===");

      logger.error("Forcefully terminating JVM now...");
      exitFn.accept(EXIT_CODE_FORCED_TERMINATION);
    } catch (InterruptedException ie) {
      if (logger.isDebugEnabled()) {
        logger.debug("Shutdown Watchdog interrupted, assuming normal driver shutdown.", ie);
      } else {
        logger.warn("Shutdown Watchdog interrupted, assuming normal driver shutdown.");
      }
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      logger.error(
          "Shutdown Watchdog error: {}: {}", t.getClass().getSimpleName(), t.getMessage(), t);
      logger.error("Proceeding with emergency termination...");
      exitFn.accept(EXIT_CODE_WATCHDOG_FAILURE);
    } finally {
      WATCHDOG_THREAD_REF.compareAndSet(Thread.currentThread(), null);
    }
  }

  /** Test-only helper to check whether the watchdog thread is currently running. */
  @VisibleForTesting
  static boolean isRunningForTests() {
    Thread t = WATCHDOG_THREAD_REF.get();
    return t != null && t.isAlive();
  }

  /**
   * Test-only helper to clean up any leftover watchdog thread and restore the default exit hook.
   */
  @VisibleForTesting
  static void resetForTests() {
    Thread thread = WATCHDOG_THREAD_REF.getAndSet(null);
    if (thread != null) {
      thread.interrupt();
      try {
        thread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    exitFn = System::exit;
  }
}
