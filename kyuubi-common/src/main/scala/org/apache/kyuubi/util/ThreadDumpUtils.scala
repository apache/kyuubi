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

import java.io.{PrintStream, PrintWriter, StringWriter}
import java.lang.management.{ManagementFactory, ThreadInfo}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._
import scala.util.Try

import org.slf4j.Logger

/**
 * Utility for generating comprehensive JVM thread dumps with various configuration options.
 *
 * This utility provides detailed thread information including:
 * - Thread states and statistics
 * - Deadlock detection
 * - Lock information
 * - Stack traces with configurable depth
 * - Separate handling of daemon vs non-daemon threads
 */
object ThreadDumpUtils {

  /**
   * Configuration for thread dump generation
   *
   * @param stackDepth Maximum number of stack frames to show per thread (0 = unlimited)
   * @param showDaemonThreads Whether to include daemon threads in the output
   * @param includeLocksInfo Whether to include lock and monitor information
   * @param includeSynchronizers Whether to include ownable synchronizers information
   * @param sortThreadsBy How to sort threads in the output
   */
  case class DumpConfig(
      stackDepth: Int = 10,
      showDaemonThreads: Boolean = true,
      includeLocksInfo: Boolean = true,
      includeSynchronizers: Boolean = true,
      sortThreadsBy: ThreadSortBy = ThreadSortBy.Name)

  /**
   * Thread sorting options
   */
  sealed trait ThreadSortBy
  object ThreadSortBy {
    case object Id extends ThreadSortBy
    case object Name extends ThreadSortBy
    case object State extends ThreadSortBy
  }

  private val DefaultConfig: DumpConfig = DumpConfig()

  /**
   * Enhanced thread information combining ThreadInfo with Thread object
   * to provide more comprehensive thread details
   */
  private case class ExtendedThreadInfo(
      threadInfo: ThreadInfo,
      thread: Option[Thread] // Maybe None if thread has died between collection points
  ) {
    def isDaemon: Boolean = thread.exists(_.isDaemon)
    def getName: String = threadInfo.getThreadName
    def getId: Long = threadInfo.getThreadId
    def getState: Thread.State = threadInfo.getThreadState
  }

  /**
   * Collects all thread information by merging data from ThreadMXBean and active Thread objects
   */
  private def getAllExtendedThreadInfo(
      includeLocksInfo: Boolean,
      includeSynchronizers: Boolean): Array[ExtendedThreadInfo] = {
    val threadBean = ManagementFactory.getThreadMXBean
    val allThreadInfos = threadBean.dumpAllThreads(includeLocksInfo, includeSynchronizers)

    // Create a map of active threads by ID for additional thread properties
    val activeThreadsMap: Map[Long, Thread] = Try {
      Thread.getAllStackTraces.keySet().asScala.map(t => t.getId -> t).toMap
    }.getOrElse(Map.empty)

    // Merge ThreadInfo with Thread object to get complete information
    allThreadInfos.map { threadInfo =>
      val thread = activeThreadsMap.get(threadInfo.getThreadId)
      ExtendedThreadInfo(threadInfo, thread)
    }
  }

  /**
   * Dumps thread information to console (System.err)
   */
  def dumpToConsole(config: DumpConfig = DefaultConfig): Unit = {
    dumpToStream(System.err, config)
  }

  /**
   * Generates thread dump as a string
   */
  def dumpToString(config: DumpConfig = DefaultConfig): String = {
    val stringWriter = new StringWriter(8192)
    val printWriter = new PrintWriter(stringWriter)
    try {
      dumpToWriter(printWriter, config)
      stringWriter.toString
    } finally {
      printWriter.close()
    }
  }

  /**
   * Dumps thread information to a PrintStream
   */
  private def dumpToStream(out: PrintStream, config: DumpConfig): Unit = {
    val printWriter = new PrintWriter(out, true)
    dumpToWriter(printWriter, config)
  }

  /**
   * Dumps thread information to a SLF4J Logger
   */
  def dumpToLogger(logger: Logger, config: DumpConfig = DefaultConfig): Unit = {
    try {
      val dump = dumpToString(config)
      logger.error("\n" + dump)
    } catch {
      case t: Throwable =>
        t.printStackTrace(System.err)
        dumpToConsole(config)
    }
  }

  /**
   * Core method that generates the formatted thread dump output
   */
  private def dumpToWriter(writer: PrintWriter, config: DumpConfig): Unit = {
    // scalastyle:off println
    def writeLine(line: String = ""): Unit = writer.println(line)
    // scalastyle:on println

    try {
      val allExtendedThreads =
        getAllExtendedThreadInfo(config.includeLocksInfo, config.includeSynchronizers)
      val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      // Header section
      writeLine("================== Thread Dump Start ==================")
      writeLine(s"Timestamp: $timestamp")
      writeLine(s"Total threads: ${allExtendedThreads.length}")
      writeLine()

      // Critical: Check for deadlocks first
      checkDeadlocks(writeLine)

      // Overview statistics
      showThreadStatistics(allExtendedThreads, writeLine)

      // Separate threads by daemon status - non-daemon threads are more critical
      val (nonDaemonThreads, daemonThreads) = allExtendedThreads.partition(!_.isDaemon)

      // Non-daemon threads section (these prevent JVM shutdown)
      writeLine()
      writeLine("==================== Non-Daemon Threads ====================")
      writeLine("(These threads prevent JVM from exiting)")
      showThreadDetails(nonDaemonThreads, config, writeLine)

      // Daemon threads section (optional)
      if (config.showDaemonThreads) {
        writeLine()
        writeLine("====================== Daemon Threads ======================")
        showThreadDetails(daemonThreads, config, writeLine)
      }

      // Summary table
      writeLine()
      writeLine("======================== Summary ========================")
      showThreadSummary(allExtendedThreads, writeLine)
      writeLine("================== Thread Dump End ==================")

    } catch {
      case t: Throwable =>
        writeLine(s"*** ERROR: Failed to generate thread dump: ${t.getMessage} ***")
        t.printStackTrace(writer)

        // Emergency fallback - provide basic thread information
        performEmergencyDump(writeLine)
    }
  }

  /**
   * Emergency fallback method when main thread dump fails
   */
  private def performEmergencyDump(writeLine: String => Unit): Unit = {
    Try {
      val basicThreads = Thread.getAllStackTraces.keySet().toArray(new Array[Thread](0))
      writeLine(s"*** Emergency fallback: Found ${basicThreads.length} threads ***")
      basicThreads.foreach { thread =>
        val threadType = if (thread.isDaemon) "daemon" else "user"
        writeLine(s"Thread: ${thread.getName} [${thread.getState}] $threadType")
      }
    }.recover { case ex =>
      writeLine(s"*** Even emergency fallback failed: ${ex.getMessage} ***")
    }
  }

  /**
   * Detects and reports deadlocks
   */
  private def checkDeadlocks(writeLine: String => Unit): Unit = {
    Try {
      val threadBean = ManagementFactory.getThreadMXBean
      val deadlockedThreads = threadBean.findDeadlockedThreads()
      if (deadlockedThreads != null && deadlockedThreads.nonEmpty) {
        writeLine("*** DEADLOCK DETECTED ***")
        writeLine(s"Deadlocked thread IDs: ${deadlockedThreads.mkString(", ")}")
        writeLine("")
      }
    }.recover { case ex =>
      writeLine(s"Warning: Could not check for deadlocks: ${ex.getMessage}")
    }
  }

  /**
   * Shows high-level thread statistics and state distribution
   */
  private def showThreadStatistics(
      allThreads: Array[ExtendedThreadInfo],
      writeLine: String => Unit): Unit = {
    val threadsByState = allThreads.groupBy(_.getState.toString)
    val (nonDaemonThreads, daemonThreads) = allThreads.partition(!_.isDaemon)

    writeLine("Thread Statistics:")
    writeLine(f"  Non-daemon threads: ${nonDaemonThreads.length}%3d")
    writeLine(f"  Daemon threads:     ${daemonThreads.length}%3d")
    writeLine("")
    writeLine("Threads by state:")

    // Sort states alphabetically for consistent output
    threadsByState.toSeq.sortBy(_._1).foreach { case (state, threads) =>
      writeLine(f"  $state%-15s: ${threads.length}%3d")
    }
  }

  /**
   * Shows detailed information for each thread including stack traces
   */
  private def showThreadDetails(
      threads: Array[ExtendedThreadInfo],
      config: DumpConfig,
      writeLine: String => Unit): Unit = {
    if (threads.isEmpty) {
      writeLine("  (No threads in this category)")
      return
    }

    // Sort threads according to configuration
    val sortedThreads = config.sortThreadsBy match {
      case ThreadSortBy.Id => threads.sortBy(_.getId)
      case ThreadSortBy.Name => threads.sortBy(_.getName)
      case ThreadSortBy.State => threads.sortBy(t => (t.getState.toString, t.getName))
    }

    sortedThreads.foreach { extThreadInfo =>
      val threadInfo = extThreadInfo.threadInfo

      // Thread header
      val daemonLabel = if (extThreadInfo.isDaemon) "daemon" else ""
      writeLine("")
      writeLine(
        s"""Thread: "${threadInfo.getThreadName}" #${threadInfo.getThreadId} $daemonLabel""")
      writeLine(s"   State: ${threadInfo.getThreadState}")

      // Lock information (if enabled and available)
      if (config.includeLocksInfo) {
        Option(threadInfo.getLockName).foreach { lockName =>
          writeLine(s"   Waiting on: <$lockName>")
        }
        Option(threadInfo.getLockOwnerName).foreach { ownerName =>
          writeLine(s"""   Lock owned by "${ownerName}" #${threadInfo.getLockOwnerId}""")
        }
      }

      // Stack trace with depth limit
      val stackTrace = threadInfo.getStackTrace
      val actualDepth = if (config.stackDepth <= 0) stackTrace.length
      else math.min(config.stackDepth, stackTrace.length)

      stackTrace.take(actualDepth).foreach { element =>
        writeLine(s"        at $element")
      }

      // Indicate if stack trace was truncated
      if (stackTrace.length > actualDepth) {
        writeLine(s"        ... (${stackTrace.length - actualDepth} more stack frames)")
      }
    }
  }

  /**
   * Shows a compact summary table of all threads
   */
  private def showThreadSummary(
      allThreads: Array[ExtendedThreadInfo],
      writeLine: String => Unit): Unit = {
    writeLine("Thread ID | Type | State           | Name")
    writeLine("----------|------|-----------------|---------------------------")

    allThreads.sortBy(_.getId).foreach { extThreadInfo =>
      val threadType = if (extThreadInfo.isDaemon) "D" else "U" // D=Daemon, U=User
      val line =
        f"${extThreadInfo.getId}%8d | $threadType%4s | " +
          f"${extThreadInfo.getState}%-15s | ${extThreadInfo.getName}"
      writeLine(line)
    }
  }
}
