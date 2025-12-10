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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 * Utility for generating JVM thread dumps, implemented purely in Java to avoid a Scala dependency.
 *
 * <p>This utility provides methods to generate comprehensive thread dumps including thread states,
 * stack traces, lock information, and deadlock detection. All methods are thread-safe and can be
 * called from any thread.
 */
public final class ThreadDumpUtils {

  private static final int STACK_DEPTH = 32;
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);

  private ThreadDumpUtils() {}

  /**
   * Dumps all thread information to the console (stderr).
   *
   * <p>This method generates a comprehensive thread dump including:
   *
   * <ul>
   *   <li>Thread statistics (daemon vs non-daemon, by state)
   *   <li>Deadlock detection
   *   <li>Detailed thread information with stack traces
   *   <li>Thread summary table
   * </ul>
   */
  public static void dumpToConsole() {
    dumpToStream(System.err);
  }

  /**
   * Generates a thread dump and returns it as a string.
   *
   * @return a formatted string containing the complete thread dump
   */
  public static String dumpToString() {
    StringWriter writer = new StringWriter(8192);
    PrintWriter printWriter = new PrintWriter(writer);
    try {
      dumpToWriter(printWriter);
      return writer.toString();
    } finally {
      printWriter.close();
    }
  }

  /**
   * Generates a thread dump and logs it using the provided logger.
   *
   * <p>If an error occurs during dump generation, it will fall back to console output.
   *
   * @param logger the logger to use for output (must not be null)
   * @throws NullPointerException if logger is null
   */
  public static void dumpToLogger(Logger logger) {
    Objects.requireNonNull(logger, "logger");
    try {
      String dump = dumpToString();
      logger.error("\n{}", dump);
    } catch (RuntimeException | Error e) {
      logger.error("Failed to generate thread dump", e);
      try {
        dumpToConsole();
      } catch (RuntimeException | Error fallbackError) {
        logger.error("Fallback thread dump also failed", fallbackError);
      }
    }
  }

  private static void dumpToStream(PrintStream out) {
    PrintWriter printWriter = new PrintWriter(out, true);
    dumpToWriter(printWriter);
  }

  private static void dumpToWriter(PrintWriter writer) {
    try {
      ExtendedThreadInfo[] allExtendedThreads = getAllExtendedThreadInfo();

      String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);

      writeLine(writer, "================== Thread Dump Start ==================");
      writeLine(writer, "Timestamp: " + timestamp);
      writeLine(writer, "Total threads: " + allExtendedThreads.length);
      writeLine(writer);

      checkDeadlocks(writer);
      showThreadStatistics(allExtendedThreads, writer);

      ThreadBuckets buckets = splitDaemonThreads(allExtendedThreads);

      writeLine(writer);
      writeLine(writer, "==================== Non-Daemon Threads ====================");
      writeLine(writer, "(These threads prevent JVM from exiting)");
      showThreadDetails(buckets.nonDaemonThreads, writer);

      writeLine(writer);
      writeLine(writer, "====================== Daemon Threads ======================");
      showThreadDetails(buckets.daemonThreads, writer);

      writeLine(writer);
      writeLine(writer, "======================== Summary ========================");
      showThreadSummary(allExtendedThreads, writer);
      writeLine(writer, "================== Thread Dump End ==================");
    } catch (RuntimeException | Error e) {
      writeLine(writer, "*** ERROR: Failed to generate thread dump: " + e.getMessage() + " ***");
      e.printStackTrace(writer);
      performEmergencyDump(writer);
    }
  }

  private static void writeLine(PrintWriter writer, String text) {
    writer.println(text);
  }

  private static void writeLine(PrintWriter writer) {
    writer.println();
  }

  private static ExtendedThreadInfo[] getAllExtendedThreadInfo() {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] allThreadInfos = threadBean.dumpAllThreads(true, true);
    Map<Long, Thread> activeThreadsMap = captureActiveThreads();

    ExtendedThreadInfo[] extendedInfos = new ExtendedThreadInfo[allThreadInfos.length];
    for (int i = 0; i < allThreadInfos.length; i++) {
      ThreadInfo threadInfo = allThreadInfos[i];
      Thread thread = activeThreadsMap.get(threadInfo.getThreadId());
      extendedInfos[i] = new ExtendedThreadInfo(threadInfo, thread);
    }
    return extendedInfos;
  }

  private static Map<Long, Thread> captureActiveThreads() {
    try {
      Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
      Map<Long, Thread> result = new LinkedHashMap<>(stackTraces.size());
      for (Map.Entry<Thread, StackTraceElement[]> entry : stackTraces.entrySet()) {
        result.put(entry.getKey().getId(), entry.getKey());
      }
      return result;
    } catch (SecurityException se) {
      return Collections.emptyMap();
    }
  }

  private static void checkDeadlocks(PrintWriter writer) {
    try {
      ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
      long[] deadlockedThreads = threadBean.findDeadlockedThreads();
      if (deadlockedThreads != null && deadlockedThreads.length > 0) {
        writeLine(writer, "*** DEADLOCK DETECTED ***");
        String ids =
            Arrays.stream(deadlockedThreads)
                .mapToObj(Long::toString)
                .collect(Collectors.joining(", "));
        writeLine(writer, "Deadlocked thread IDs: " + ids);
        writeLine(writer);
      }
    } catch (SecurityException e) {
      writeLine(writer, "Warning: Could not check for deadlocks: " + e.getMessage());
    } catch (RuntimeException | Error e) {
      writeLine(writer, "Warning: Could not check for deadlocks: " + e.getMessage());
    }
  }

  private static void showThreadStatistics(ExtendedThreadInfo[] allThreads, PrintWriter writer) {
    Map<Thread.State, List<ExtendedThreadInfo>> byState = new EnumMap<>(Thread.State.class);
    for (ExtendedThreadInfo info : allThreads) {
      byState.computeIfAbsent(info.getState(), k -> new ArrayList<>()).add(info);
    }

    ThreadBuckets buckets = splitDaemonThreads(allThreads);

    writeLine(writer, "Thread Statistics:");
    writeLine(
        writer,
        String.format(Locale.ROOT, "  Non-daemon threads: %3d", buckets.nonDaemonThreads.length));
    writeLine(
        writer,
        String.format(Locale.ROOT, "  Daemon threads:     %3d", buckets.daemonThreads.length));
    writeLine(writer);
    writeLine(writer, "Threads by state:");

    byState.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry ->
                writeLine(
                    writer,
                    String.format(
                        Locale.ROOT, "  %-15s: %3d", entry.getKey(), entry.getValue().size())));
  }

  private static void showThreadDetails(ExtendedThreadInfo[] threads, PrintWriter writer) {
    if (threads.length == 0) {
      writeLine(writer, "  (No threads in this category)");
      return;
    }

    ExtendedThreadInfo[] sorted = Arrays.copyOf(threads, threads.length);
    Arrays.sort(sorted, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));

    for (ExtendedThreadInfo info : sorted) {
      ThreadInfo threadInfo = info.getThreadInfo();

      String daemonLabel = info.isDaemon() ? "daemon" : "";
      writeLine(writer);
      writeLine(
          writer,
          String.format(
              Locale.ROOT,
              "Thread: \"%s\" #%d %s",
              threadInfo.getThreadName(),
              threadInfo.getThreadId(),
              daemonLabel));
      writeLine(writer, "   State: " + threadInfo.getThreadState());

      if (threadInfo.getLockName() != null) {
        writeLine(writer, "   Waiting on: <" + threadInfo.getLockName() + ">");
      }
      if (threadInfo.getLockOwnerName() != null) {
        writeLine(
            writer,
            String.format(
                Locale.ROOT,
                "   Lock owned by \"%s\" #%d",
                threadInfo.getLockOwnerName(),
                threadInfo.getLockOwnerId()));
      }

      StackTraceElement[] stackTrace = threadInfo.getStackTrace();
      int actualDepth = Math.min(STACK_DEPTH, stackTrace.length);

      for (int i = 0; i < actualDepth; i++) {
        writeLine(writer, "        at " + stackTrace[i]);
      }

      if (stackTrace.length > actualDepth) {
        writeLine(
            writer,
            String.format(
                Locale.ROOT,
                "        ... (%d more stack frames)",
                stackTrace.length - actualDepth));
      }
    }
  }

  private static void showThreadSummary(ExtendedThreadInfo[] allThreads, PrintWriter writer) {
    writeLine(writer, "Thread ID | Type | State           | Name");
    writeLine(writer, "----------|------|-----------------|---------------------------");

    ExtendedThreadInfo[] sorted = Arrays.copyOf(allThreads, allThreads.length);
    Arrays.sort(sorted, (a, b) -> Long.compare(a.getId(), b.getId()));

    for (ExtendedThreadInfo info : sorted) {
      char type = info.isDaemon() ? 'D' : 'U';
      writeLine(
          writer,
          String.format(
              Locale.ROOT,
              "%8d | %4s | %-15s | %s",
              info.getId(),
              String.valueOf(type),
              info.getState(),
              info.getName()));
    }
  }

  private static void performEmergencyDump(PrintWriter writer) {
    try {
      Thread[] basicThreads = Thread.getAllStackTraces().keySet().toArray(new Thread[0]);
      writeLine(writer, "*** Emergency fallback: Found " + basicThreads.length + " threads ***");
      for (Thread thread : basicThreads) {
        String type = thread.isDaemon() ? "daemon" : "user";
        writeLine(
            writer,
            String.format(
                Locale.ROOT, "Thread: %s [%s] %s", thread.getName(), thread.getState(), type));
      }
    } catch (SecurityException e) {
      writeLine(writer, "*** Even emergency fallback failed: " + e.getMessage() + " ***");
    } catch (RuntimeException | Error e) {
      writeLine(writer, "*** Even emergency fallback failed: " + e.getMessage() + " ***");
    }
  }

  private static ThreadBuckets splitDaemonThreads(ExtendedThreadInfo[] allThreads) {
    List<ExtendedThreadInfo> daemon = new ArrayList<>();
    List<ExtendedThreadInfo> nonDaemon = new ArrayList<>();
    for (ExtendedThreadInfo info : allThreads) {
      if (info.isDaemon()) {
        daemon.add(info);
      } else {
        nonDaemon.add(info);
      }
    }
    return new ThreadBuckets(
        daemon.toArray(new ExtendedThreadInfo[0]), nonDaemon.toArray(new ExtendedThreadInfo[0]));
  }

  private static final class ThreadBuckets {
    final ExtendedThreadInfo[] daemonThreads;
    final ExtendedThreadInfo[] nonDaemonThreads;

    ThreadBuckets(ExtendedThreadInfo[] daemonThreads, ExtendedThreadInfo[] nonDaemonThreads) {
      this.daemonThreads = daemonThreads;
      this.nonDaemonThreads = nonDaemonThreads;
    }
  }

  private static final class ExtendedThreadInfo {
    private final ThreadInfo threadInfo;
    private final Thread thread;

    ExtendedThreadInfo(ThreadInfo threadInfo, Thread thread) {
      this.threadInfo = threadInfo;
      this.thread = thread;
    }

    ThreadInfo getThreadInfo() {
      return threadInfo;
    }

    boolean isDaemon() {
      return thread != null && thread.isDaemon();
    }

    String getName() {
      return threadInfo.getThreadName();
    }

    long getId() {
      return threadInfo.getThreadId();
    }

    Thread.State getState() {
      return threadInfo.getThreadState();
    }
  }
}
