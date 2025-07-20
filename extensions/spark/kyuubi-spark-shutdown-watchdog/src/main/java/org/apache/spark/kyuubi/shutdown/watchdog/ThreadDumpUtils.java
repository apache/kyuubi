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
 */
public final class ThreadDumpUtils {

  private static final DumpConfig DEFAULT_CONFIG = new DumpConfig();
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);

  private ThreadDumpUtils() {}

  /** Immutable configuration for thread dump generation with builder-style helpers. */
  public static final class DumpConfig {

    private final int stackDepth;
    private final boolean showDaemonThreads;
    private final boolean includeLocksInfo;
    private final boolean includeSynchronizers;
    private final ThreadSortBy sortThreadsBy;

    public DumpConfig() {
      this(10, true, true, true, ThreadSortBy.NAME);
    }

    private DumpConfig(
        int stackDepth,
        boolean showDaemonThreads,
        boolean includeLocksInfo,
        boolean includeSynchronizers,
        ThreadSortBy sortThreadsBy) {
      this.stackDepth = stackDepth;
      this.showDaemonThreads = showDaemonThreads;
      this.includeLocksInfo = includeLocksInfo;
      this.includeSynchronizers = includeSynchronizers;
      this.sortThreadsBy = Objects.requireNonNull(sortThreadsBy, "sortThreadsBy");
    }

    public DumpConfig withStackDepth(int newStackDepth) {
      return new DumpConfig(
          newStackDepth, showDaemonThreads, includeLocksInfo, includeSynchronizers, sortThreadsBy);
    }

    public DumpConfig withShowDaemonThreads(boolean showDaemon) {
      return new DumpConfig(
          stackDepth, showDaemon, includeLocksInfo, includeSynchronizers, sortThreadsBy);
    }

    public DumpConfig withIncludeLocksInfo(boolean includeLocks) {
      return new DumpConfig(
          stackDepth, showDaemonThreads, includeLocks, includeSynchronizers, sortThreadsBy);
    }

    public DumpConfig withIncludeSynchronizers(boolean includeSync) {
      return new DumpConfig(
          stackDepth, showDaemonThreads, includeLocksInfo, includeSync, sortThreadsBy);
    }

    public DumpConfig withSortThreadsBy(ThreadSortBy sortBy) {
      return new DumpConfig(
          stackDepth, showDaemonThreads, includeLocksInfo, includeSynchronizers, sortBy);
    }

    public int getStackDepth() {
      return stackDepth;
    }

    public boolean isShowDaemonThreads() {
      return showDaemonThreads;
    }

    public boolean isIncludeLocksInfo() {
      return includeLocksInfo;
    }

    public boolean isIncludeSynchronizers() {
      return includeSynchronizers;
    }

    public ThreadSortBy getSortThreadsBy() {
      return sortThreadsBy;
    }
  }

  public enum ThreadSortBy {
    ID,
    NAME,
    STATE
  }

  public static void dumpToConsole() {
    dumpToConsole(DEFAULT_CONFIG);
  }

  public static void dumpToConsole(DumpConfig config) {
    dumpToStream(System.err, config);
  }

  public static String dumpToString() {
    return dumpToString(DEFAULT_CONFIG);
  }

  public static String dumpToString(DumpConfig config) {
    StringWriter writer = new StringWriter(8192);
    PrintWriter printWriter = new PrintWriter(writer);
    try {
      dumpToWriter(printWriter, config);
      return writer.toString();
    } finally {
      printWriter.close();
    }
  }

  public static void dumpToLogger(Logger logger) {
    dumpToLogger(logger, DEFAULT_CONFIG);
  }

  public static void dumpToLogger(Logger logger, DumpConfig config) {
    Objects.requireNonNull(logger, "logger");
    try {
      String dump = dumpToString(config);
      logger.error("\n{}", dump);
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      dumpToConsole(config);
    }
  }

  private static void dumpToStream(PrintStream out, DumpConfig config) {
    PrintWriter printWriter = new PrintWriter(out, true);
    dumpToWriter(printWriter, config);
  }

  private static void dumpToWriter(PrintWriter writer, DumpConfig config) {
    DumpConfig effectiveConfig = config != null ? config : DEFAULT_CONFIG;

    try {
      ExtendedThreadInfo[] allExtendedThreads =
          getAllExtendedThreadInfo(
              effectiveConfig.isIncludeLocksInfo(), effectiveConfig.isIncludeSynchronizers());

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
      showThreadDetails(buckets.nonDaemonThreads, effectiveConfig, writer);

      if (effectiveConfig.isShowDaemonThreads()) {
        writeLine(writer);
        writeLine(writer, "====================== Daemon Threads ======================");
        showThreadDetails(buckets.daemonThreads, effectiveConfig, writer);
      }

      writeLine(writer);
      writeLine(writer, "======================== Summary ========================");
      showThreadSummary(allExtendedThreads, writer);
      writeLine(writer, "================== Thread Dump End ==================");
    } catch (Throwable t) {
      writeLine(writer, "*** ERROR: Failed to generate thread dump: " + t.getMessage() + " ***");
      t.printStackTrace(writer);
      performEmergencyDump(writer);
    }
  }

  private static void writeLine(PrintWriter writer) {
    writer.println();
  }

  private static void writeLine(PrintWriter writer, String text) {
    writer.println(text);
  }

  private static ExtendedThreadInfo[] getAllExtendedThreadInfo(
      boolean includeLocksInfo, boolean includeSynchronizers) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] allThreadInfos = threadBean.dumpAllThreads(includeLocksInfo, includeSynchronizers);

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
      for (Thread thread : stackTraces.keySet()) {
        result.put(thread.getId(), thread);
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
        writeLine(
            writer,
            "Deadlocked thread IDs: "
                + Arrays.stream(deadlockedThreads)
                    .mapToObj(Long::toString)
                    .collect(Collectors.joining(", ")));
        writeLine(writer);
      }
    } catch (Throwable t) {
      writeLine(writer, "Warning: Could not check for deadlocks: " + t.getMessage());
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

  private static void showThreadDetails(
      ExtendedThreadInfo[] threads, DumpConfig config, PrintWriter writer) {
    if (threads.length == 0) {
      writeLine(writer, "  (No threads in this category)");
      return;
    }

    ExtendedThreadInfo[] sorted = Arrays.copyOf(threads, threads.length);
    switch (config.getSortThreadsBy()) {
      case ID:
        Arrays.sort(sorted, (a, b) -> Long.compare(a.getId(), b.getId()));
        break;
      case NAME:
        Arrays.sort(sorted, (a, b) -> a.getName().compareTo(b.getName()));
        break;
      case STATE:
        Arrays.sort(
            sorted,
            (a, b) -> {
              int stateCompare = a.getState().compareTo(b.getState());
              return stateCompare != 0 ? stateCompare : a.getName().compareTo(b.getName());
            });
        break;
      default:
        break;
    }

    for (ExtendedThreadInfo info : sorted) {
      ThreadInfo threadInfo = info.threadInfo;

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

      if (config.isIncludeLocksInfo()) {
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
      }

      StackTraceElement[] stackTrace = threadInfo.getStackTrace();
      int actualDepth =
          config.getStackDepth() <= 0
              ? stackTrace.length
              : Math.min(config.getStackDepth(), stackTrace.length);

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
    } catch (Throwable t) {
      writeLine(writer, "*** Even emergency fallback failed: " + t.getMessage() + " ***");
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
