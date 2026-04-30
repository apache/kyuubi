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

package org.apache.kyuubi.engine.dataagent.runtime.middleware;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ToolOutputStore;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.output.GrepToolOutputTool;
import org.apache.kyuubi.engine.dataagent.tool.output.ReadToolOutputTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input-gate middleware that offloads oversized tool outputs to per-session temp files and replaces
 * the in-memory tool result with a small head + tail preview plus a retrieval hint. The cheapest
 * and highest-ROI defense against context rot.
 *
 * <p><b>Trigger:</b> {@code result.lines > MAX_LINES} OR {@code result.bytes > MAX_BYTES}, first to
 * trip wins. Thresholds are hardcoded — the ReAct loop can compensate for suboptimal defaults by
 * calling the retrieval tools more aggressively.
 *
 * <p><b>Exempt tools:</b> {@link ReadToolOutputTool} and {@link GrepToolOutputTool} never go
 * through the gate — the agent would otherwise recursively re-offload its own retrieval output.
 *
 * <p><b>Session lifecycle:</b> a monotonic counter per session is used to name temp files. {@link
 * #onSessionClose(String)} wipes the counter and the per-session temp dir; call it from the
 * provider's {@code close(sessionId)} hook (not from {@link #onAgentFinish}, which fires on every
 * turn and would invalidate paths the LLM still needs to reference).
 */
public class ToolResultOffloadMiddleware implements AgentMiddleware {

  private static final Logger LOG = LoggerFactory.getLogger(ToolResultOffloadMiddleware.class);

  static final int MAX_LINES = 500;
  static final int MAX_BYTES = 50 * 1024;
  static final int PREVIEW_HEAD_LINES = 20;
  static final int PREVIEW_TAIL_LINES = 20;

  private static final Set<String> EXEMPT_TOOLS =
      new HashSet<>(Arrays.asList(ReadToolOutputTool.NAME, GrepToolOutputTool.NAME));

  private final ToolOutputStore store = ToolOutputStore.create();
  private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

  /**
   * Register the companion retrieval tools so the LLM can reach back into offloaded files. Paired
   * with the preview hint emitted from {@link #afterToolCall}; skipping this registration would
   * leave the LLM dangling on file paths it can never read.
   */
  @Override
  public void onRegister(ToolRegistry registry) {
    registry.register(new ReadToolOutputTool(store));
    registry.register(new GrepToolOutputTool(store));
  }

  @Override
  public Decision<String> afterToolCall(AgentRunContext ctx, ToolInvocation call, String result) {
    if (result.isEmpty()) return Decision.proceed();
    String toolName = call.name();
    if (EXEMPT_TOOLS.contains(toolName)) return Decision.proceed();

    int bytes = result.getBytes(StandardCharsets.UTF_8).length;
    int lines = countLines(result);
    if (lines <= MAX_LINES && bytes <= MAX_BYTES) {
      return Decision.proceed();
    }

    // AgentRunContext.sessionId is null in unit-test constructions that don't exercise offload.
    // In production the provider always threads it through, so treat null as "skip offload".
    String sessionId = ctx.getSessionId();
    if (sessionId == null) return Decision.proceed();

    long n = counters.computeIfAbsent(sessionId, k -> new AtomicLong()).incrementAndGet();
    String toolCallId = toolName + "_" + n;

    Path file;
    try {
      file = store.write(sessionId, toolCallId, result);
    } catch (IOException e) {
      LOG.warn(
          "Tool output offload failed for tool={} session={}; passing through full output",
          toolName,
          sessionId,
          e);
      return Decision.proceed();
    }

    LOG.info(
        "Offloaded tool={} session={} ({} lines / {} bytes) -> {}",
        toolName,
        sessionId,
        lines,
        bytes,
        file.getFileName());
    return Decision.replace(buildPreview(result, lines, bytes, file));
  }

  /** Clean up counter and temp dir for a closed session. Idempotent. */
  @Override
  public void onSessionClose(String sessionId) {
    if (sessionId == null) return;
    counters.remove(sessionId);
    store.cleanupSession(sessionId);
  }

  /** Engine-wide shutdown: drop the temp root. */
  @Override
  public void onStop() {
    store.close();
  }

  static int countLines(String s) {
    if (s.isEmpty()) return 0;
    int count = 1;
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == '\n') count++;
    }
    // Trailing newline means the last "line" is empty, but still counted. Good enough for
    // gating decisions — we're not trying to match `wc -l` exactly.
    return count;
  }

  static String buildPreview(String full, int lines, int bytes, Path file) {
    String[] split = full.split("\n", -1);
    int headEnd = Math.min(PREVIEW_HEAD_LINES, split.length);
    int tailStart = Math.max(headEnd, split.length - PREVIEW_TAIL_LINES);

    StringBuilder sb = new StringBuilder();
    sb.append("[Tool output truncated: ")
        .append(lines)
        .append(" lines, ")
        .append(humanBytes(bytes))
        .append("]\n")
        .append("Saved to: ")
        .append(file.toString())
        .append("\n\n--- First ")
        .append(headEnd)
        .append(" lines ---\n");
    for (int i = 0; i < headEnd; i++) {
      sb.append(split[i]).append('\n');
    }
    if (tailStart < split.length) {
      int tailCount = split.length - tailStart;
      sb.append("--- Last ").append(tailCount).append(" lines ---\n");
      for (int i = tailStart; i < split.length; i++) {
        sb.append(split[i]).append('\n');
      }
    }
    sb.append("\nUse ")
        .append(ReadToolOutputTool.NAME)
        .append("(path, offset, limit) to read windows, or ")
        .append(GrepToolOutputTool.NAME)
        .append("(path, pattern, max_matches) to search.");
    return sb.toString();
  }

  private static String humanBytes(long bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
    return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
  }

  /** Visible for testing. */
  int trackedSessions() {
    return counters.size();
  }
}
