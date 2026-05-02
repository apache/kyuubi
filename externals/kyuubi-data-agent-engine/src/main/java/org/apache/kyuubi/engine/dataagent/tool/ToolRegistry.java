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

package org.apache.kyuubi.engine.dataagent.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.core.JsonValue;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionFunctionTool;
import com.openai.models.chat.completions.ChatCompletionTool;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry for agent tools. Provides tool lookup, schema generation for LLM requests, and
 * deserialization + execution of tool calls.
 *
 * <p>Tool metadata (name, description) comes from {@link AgentTool}. Parameter schemas are
 * generated from the args class via {@link ToolSchemaGenerator}. The SDK's {@code
 * function.arguments(Class)} is used for deserializing LLM responses.
 *
 * <p><b>Concurrency model: single-writer at startup.</b> {@link #register} is expected to be called
 * only during engine initialization, before any LLM request is dispatched. After that the registry
 * is read-only. {@link #addToolsTo} therefore does not synchronize on the {@code tools} map, which
 * is safe under this single-writer-at-startup assumption — do NOT "fix" this by adding a lock
 * around the read path. If we ever need true runtime registration, the right move is to swap {@code
 * tools} for an immutable snapshot per refresh, not to grab a mutex on the hot path.
 */
public class ToolRegistry implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ToolRegistry.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  /**
   * Hard ceiling on concurrent tool-call workers. Sized for the realistic working set: one LLM turn
   * rarely dispatches more than a handful of parallel {@code tool_calls}, and a Kyuubi data-agent
   * engine serves a small number of overlapping sessions. When the ceiling is reached we reject
   * fast and tell the LLM to retry — better UX than burying the request in a queue for minutes.
   * Tuned as a safety rail, not a user knob.
   */
  private static final int MAX_POOL_SIZE = 8;

  /** Default wall-clock cap for a single tool call, used when no explicit value is configured. */
  public static final long DEFAULT_TIMEOUT_SECONDS = 300;

  private final Map<String, AgentTool<?>> tools = new LinkedHashMap<>();
  private volatile Map<String, ChatCompletionTool> cachedSpecs;
  private final long toolCallTimeoutSeconds;
  private final ExecutorService executor;
  private final ScheduledExecutorService timeoutScheduler;

  /**
   * @param toolCallTimeoutSeconds wall-clock cap on every {@link #executeTool} / {@link
   *     #submitTool} call, sourced from {@code kyuubi.engine.data.agent.tool.call.timeout}. When
   *     the timeout fires, the worker thread is interrupted and a descriptive error is returned to
   *     the LLM.
   */
  public ToolRegistry(long toolCallTimeoutSeconds) {
    this.toolCallTimeoutSeconds = toolCallTimeoutSeconds;
    AtomicLong threadId = new AtomicLong();
    this.executor =
        new ThreadPoolExecutor(
            0,
            MAX_POOL_SIZE,
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            r -> {
              Thread t = new Thread(r, "tool-call-worker-" + threadId.incrementAndGet());
              t.setDaemon(true);
              return t;
            });
    this.timeoutScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "tool-call-timeout");
              t.setDaemon(true);
              return t;
            });
  }

  /** Shut down the worker pool and the timeout scheduler. Idempotent. */
  @Override
  public void close() {
    executor.shutdownNow();
    timeoutScheduler.shutdownNow();
  }

  /** Register a tool. Keyed by {@link AgentTool#name()}. */
  public synchronized ToolRegistry register(AgentTool<?> tool) {
    tools.put(tool.name(), tool);
    cachedSpecs = null; // invalidate cache
    return this;
  }

  public synchronized boolean isEmpty() {
    return tools.isEmpty();
  }

  /** Returns the risk level of the named tool, or {@link ToolRiskLevel#SAFE} if not found. */
  public synchronized ToolRiskLevel getRiskLevel(String toolName) {
    AgentTool<?> tool = tools.get(toolName);
    return tool != null ? tool.riskLevel() : ToolRiskLevel.SAFE;
  }

  /** Add all tools to the ChatCompletion request builder. */
  public void addToolsTo(ChatCompletionCreateParams.Builder builder) {
    Map<String, ChatCompletionTool> specs = ensureSpecs();
    for (ChatCompletionTool spec : specs.values()) {
      builder.addTool(spec);
    }
  }

  private synchronized Map<String, ChatCompletionTool> ensureSpecs() {
    if (cachedSpecs == null) {
      Map<String, ChatCompletionTool> specs = new LinkedHashMap<>();
      for (AgentTool<?> tool : tools.values()) {
        specs.put(tool.name(), buildChatCompletionTool(tool));
      }
      cachedSpecs = specs;
    }
    return cachedSpecs;
  }

  /**
   * Synchronous entry point. Blocks until the tool finishes, times out, or the registry rejects the
   * submission. Errors are surfaced as strings (never as exceptions) so the LLM can observe and
   * react to them.
   *
   * @param toolName the function name from the LLM response
   * @param argsJson the raw JSON arguments string
   * @return the result string, or an error message
   */
  public String executeTool(String toolName, String argsJson) {
    return submitTool(toolName, argsJson, ToolContext.EMPTY).join();
  }

  /** Synchronous entry point with an explicit {@link ToolContext}. */
  public String executeTool(String toolName, String argsJson, ToolContext ctx) {
    return submitTool(toolName, argsJson, ctx).join();
  }

  public CompletableFuture<String> submitTool(String toolName, String argsJson) {
    return submitTool(toolName, argsJson, ToolContext.EMPTY);
  }

  /**
   * Asynchronous entry point. Deserialize args, run the tool on the worker pool, and apply a
   * wall-clock timeout sourced from {@code kyuubi.engine.data.agent.tool.call.timeout}. The
   * returned future is guaranteed to complete normally — timeouts, pool saturation, unknown tool,
   * and execution failures are all translated into error strings. Callers can therefore use {@code
   * .join()} / {@code .get()} without handling {@link java.util.concurrent.TimeoutException} or
   * {@link java.util.concurrent.ExecutionException}.
   *
   * @param toolName the function name from the LLM response
   * @param argsJson the raw JSON arguments string
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<String> submitTool(String toolName, String argsJson, ToolContext ctx) {
    AgentTool<Object> tool;
    synchronized (this) {
      tool = (AgentTool<Object>) tools.get(toolName);
    }
    if (tool == null) {
      return CompletableFuture.completedFuture("Error: unknown tool '" + toolName + "'");
    }
    ToolContext toolCtx = ctx != null ? ctx : ToolContext.EMPTY;

    CompletableFuture<String> result = new CompletableFuture<>();
    Future<?> submitted;
    try {
      submitted =
          executor.submit(
              () -> {
                try {
                  Object args = JSON.readValue(argsJson, tool.argsType());
                  String out = tool.execute(toolCtx, args);
                  // When the timeout handler interrupts us, the tool may still unwind cleanly and
                  // produce a stale return value — don't race the scheduler's timeout message with
                  // it. Let the timeout path be the single authority for the final result.
                  if (!Thread.currentThread().isInterrupted()) {
                    result.complete(out);
                  }
                } catch (Exception e) {
                  result.complete("Error executing " + toolName + ": " + e.getMessage());
                }
              });
    } catch (RejectedExecutionException e) {
      LOG.warn("Tool call '{}' rejected — worker pool saturated at {}", toolName, MAX_POOL_SIZE);
      return CompletableFuture.completedFuture(
          "Error: tool call '"
              + toolName
              + "' rejected — server is handling too many concurrent tool calls. "
              + "Retry in a moment.");
    }

    ScheduledFuture<?> timer =
        timeoutScheduler.schedule(
            () -> {
              if (!result.isDone()) {
                // cancel(true) interrupts the worker thread directly — the inner task's
                // catch-all will see the interrupt and call result.complete(...), but the
                // timeout message below wins because complete() is idempotent on first-winner.
                submitted.cancel(true);
                LOG.warn(
                    "Tool call '{}' timed out after {} seconds", toolName, toolCallTimeoutSeconds);
                result.complete(
                    "Error: tool call '"
                        + toolName
                        + "' timed out after "
                        + toolCallTimeoutSeconds
                        + " seconds. "
                        + "Try simplifying the query or adding filters to reduce execution time.");
              }
            },
            toolCallTimeoutSeconds,
            TimeUnit.SECONDS);
    result.whenComplete((r, e) -> timer.cancel(false));
    return result;
  }

  private static ChatCompletionTool buildChatCompletionTool(AgentTool<?> tool) {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(tool.argsType());

    FunctionParameters.Builder paramsBuilder = FunctionParameters.builder();
    for (Map.Entry<String, Object> entry : schema.entrySet()) {
      paramsBuilder.putAdditionalProperty(entry.getKey(), JsonValue.from(entry.getValue()));
    }

    return ChatCompletionTool.ofFunction(
        ChatCompletionFunctionTool.builder()
            .function(
                FunctionDefinition.builder()
                    .name(tool.name())
                    .description(tool.description())
                    .parameters(paramsBuilder.build())
                    .build())
            .build());
  }
}
