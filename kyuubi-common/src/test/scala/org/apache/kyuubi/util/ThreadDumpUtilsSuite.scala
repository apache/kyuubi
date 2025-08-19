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

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.kyuubi.KyuubiFunSuite

class ThreadDumpUtilsSuite extends KyuubiFunSuite {

  test("dumpToString should return non-empty thread dump") {
    val dump = ThreadDumpUtils.dumpToString()
    assert(dump != null && dump.nonEmpty)
    assert(dump.contains("Thread Dump Start"))
    assert(dump.contains("Thread Dump End"))
  }

  test("dumpToConsole should print to provided stream") {
    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos)
    val oldErr = System.err
    try {
      System.setErr(ps)
      ThreadDumpUtils.dumpToConsole()
      ps.flush()
    } finally {
      System.setErr(oldErr)
    }
    val output = baos.toString("UTF-8")
    assert(output.contains("Thread Dump Start"))
    assert(output.contains("Thread Dump End"))
  }

  test("dumpToString should respect showDaemonThreads config") {
    val config = ThreadDumpUtils.DumpConfig(showDaemonThreads = false)
    val dump = ThreadDumpUtils.dumpToString(config)
    assert(dump.contains("Thread Dump Start"))
    assert(dump.contains("Thread Dump End"))
    assert(!dump.contains("====================== Daemon Threads ======================"))
  }

  /**
   * Helper to find a specific thread's section in the dump.
   * This is useful for targeted assertions on a single thread's output.
   */
  private def findThreadSection(dump: String, threadName: String): Option[String] = {
    val pattern = s"""(?s)Thread: "$threadName".*?(?=\\n\\nThread:|\\n\\n================)""".r
    pattern.findFirstIn(dump)
  }

  test("dumpToString should correctly limit stack depth and show truncation message") {
    val readyLatch = new CountDownLatch(1)
    var deepStackThread: Thread = null
    val stackDepth = 100
    val limit = 10

    // This function is INTENTIONALLY NOT tail-recursive.
    // By performing an operation after the recursive call, we prevent the compiler
    // from optimizing it into a loop, thus forcing a deep stack trace.
    def deepStack(n: Int): Int = {
      if (n > 0) {
        val res = deepStack(n - 1)
        res + 1 // This operation breaks tail-call optimization
      } else {
        readyLatch.countDown() // Signal that the thread has reached the bottom of the stack
        try {
          Thread.sleep(5000) // Keep thread alive so we can dump it
        } catch { case _: InterruptedException => Thread.currentThread().interrupt() }
        0 // Base case return
      }
    }

    try {
      deepStackThread = new Thread(() => deepStack(stackDepth), "deep-stack-test-thread")
      deepStackThread.setDaemon(true)
      deepStackThread.start()

      assert(readyLatch.await(5, TimeUnit.SECONDS), "Test thread did not initialize in time")

      // 1. Get the full, unlimited dump to determine the actual total stack depth.
      val unlimitedConfig = ThreadDumpUtils.DumpConfig(stackDepth = 0)
      val unlimitedDump = ThreadDumpUtils.dumpToString(unlimitedConfig)
      val unlimitedSection = findThreadSection(unlimitedDump, "deep-stack-test-thread")
      assert(
        unlimitedSection.isDefined,
        "Thread 'deep-stack-test-thread' not found in unlimited dump")
      val totalFrames = unlimitedSection.get.linesIterator.count(_.trim.startsWith("at "))
      assert(totalFrames > stackDepth, "Full stack depth is not as deep as expected.")

      // 2. Get the limited dump and verify its contents against the full one.
      val limitedConfig = ThreadDumpUtils.DumpConfig(stackDepth = limit)
      val limitedDump = ThreadDumpUtils.dumpToString(limitedConfig)
      val limitedSection = findThreadSection(limitedDump, "deep-stack-test-thread")
      assert(limitedSection.isDefined, "Thread 'deep-stack-test-thread' not found in limited dump")

      // Verify the number of "at" lines matches the configured limit.
      val stackTraceLines = limitedSection.get.linesIterator.count(_.trim.startsWith("at "))
      assert(stackTraceLines == limit)

      // Verify the truncation message is present and mathematically correct.
      val expectedMoreFrames = totalFrames - limit
      assert(
        limitedSection.get.contains(s"... (${expectedMoreFrames} more stack frames)"),
        s"Dump did not contain the expected truncation message. " +
          s"Expected '... ($expectedMoreFrames more stack frames)'.")

    } finally {
      if (deepStackThread != null) deepStackThread.interrupt()
    }
  }

  test("dumpToString should sort threads by ID when configured") {
    val config = ThreadDumpUtils.DumpConfig(sortThreadsBy = ThreadDumpUtils.ThreadSortBy.Id)
    val dump = ThreadDumpUtils.dumpToString(config)

    // Extract the summary table for easier parsing
    val summarySection =
      dump.substring(dump.indexOf("======================== Summary ========================"))

    // Regex to extract thread IDs from the summary table lines
    val idPattern = """^\s*(\d+)\s*\|.*""".r
    val ids = summarySection.linesIterator.flatMap { line =>
      idPattern.findFirstMatchIn(line).map(_.group(1).toLong)
    }.toList

    assert(ids.nonEmpty, "No thread IDs found in the summary")
    // Verify that the list of IDs is sorted, which proves the sorting logic worked
    assert(ids == ids.sorted, s"Thread IDs are not sorted: $ids")
  }

  test("dumpToString should detect and report deadlocks") {
    val lock1 = new Object()
    val lock2 = new Object()
    // Latch to ensure both threads are in a deadlock state before we take the dump
    val deadlockSetupLatch = new CountDownLatch(2)

    val thread1 = new Thread(
      () => {
        lock1.synchronized {
          deadlockSetupLatch.countDown()
          Thread.sleep(200) // Wait for thread2 to acquire lock2
          lock2.synchronized {
            // This line will never be reached
          }
        }
      },
      "kyuubi-deadlock-thread-1")

    val thread2 = new Thread(
      () => {
        lock2.synchronized {
          deadlockSetupLatch.countDown()
          Thread.sleep(200) // Wait for thread1 to acquire lock1
          lock1.synchronized {
            // This line will never be reached
          }
        }
      },
      "kyuubi-deadlock-thread-2")

    // Use daemon threads so they don't block JVM exit if the test fails
    thread1.setDaemon(true)
    thread2.setDaemon(true)

    try {
      thread1.start()
      thread2.start()

      // Wait for both threads to acquire their first lock
      assert(
        deadlockSetupLatch.await(5, TimeUnit.SECONDS),
        "Deadlock condition was not met in time")

      // Give the JVM time to officially recognize the deadlock state
      Thread.sleep(500)

      val dump = ThreadDumpUtils.dumpToString()

      assert(dump.contains("*** DEADLOCK DETECTED ***"))
      // Check that both threads involved in the deadlock are mentioned
      assert(dump.contains(""""kyuubi-deadlock-thread-1""""))
      assert(dump.contains(""""kyuubi-deadlock-thread-2""""))
      // Check for lock details which are crucial for debugging deadlocks
      assert(dump.contains("Waiting on:"))
      assert(dump.contains("Lock owned by"))

    } finally {
      // Clean up the threads
      thread1.interrupt()
      thread2.interrupt()
    }
  }
}
