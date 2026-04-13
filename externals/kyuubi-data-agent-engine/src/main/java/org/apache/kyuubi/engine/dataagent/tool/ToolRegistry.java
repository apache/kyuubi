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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
public class ToolRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ToolRegistry.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final Map<String, AgentTool<?>> tools = new LinkedHashMap<>();
  private volatile Map<String, ChatCompletionTool> cachedSpecs;
  private final long toolCallTimeoutSeconds;
  private final ExecutorService executor;

  /**
   * @param toolCallTimeoutSeconds wall-clock cap on every {@link #executeTool} call, sourced from
   *     {@code kyuubi.engine.data.agent.tool.call.timeout}. When the timeout fires, the thread is
   *     interrupted and a descriptive error is returned to the LLM.
   */
  public ToolRegistry(long toolCallTimeoutSeconds) {
    this.toolCallTimeoutSeconds = toolCallTimeoutSeconds;
    this.executor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "tool-call-worker");
              t.setDaemon(true);
              return t;
            });
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
   * Execute a tool call: deserialize the JSON args, then delegate to the tool, with a wall-clock
   * timeout sourced from {@code kyuubi.engine.data.agent.tool.call.timeout}. If the tool does not
   * finish within the timeout, the worker thread is interrupted and a descriptive error is returned
   * to the LLM so it can react (e.g. simplify the query, retry with LIMIT).
   *
   * @param toolName the function name from the LLM response
   * @param argsJson the raw JSON arguments string
   * @return the result string, or an error message
   */
  @SuppressWarnings("unchecked")
  public String executeTool(String toolName, String argsJson) {
    AgentTool<?> tool;
    synchronized (this) {
      tool = tools.get(toolName);
    }
    if (tool == null) {
      return "Error: unknown tool '" + toolName + "'";
    }
    return executeWithTimeout((AgentTool<Object>) tool, argsJson);
  }

  private <T> String executeWithTimeout(AgentTool<T> tool, String argsJson) {
    Callable<String> task =
        () -> {
          T args = JSON.readValue(argsJson, tool.argsType());
          return tool.execute(args);
        };
    Future<String> future = executor.submit(task);
    try {
      return future.get(toolCallTimeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      LOG.warn("Tool call '{}' timed out after {} seconds", tool.name(), toolCallTimeoutSeconds);
      return "Error: tool call '"
          + tool.name()
          + "' timed out after "
          + toolCallTimeoutSeconds
          + " seconds. "
          + "Try simplifying the query or adding filters to reduce execution time.";
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      return "Error executing " + tool.name() + ": " + cause.getMessage();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "Error: tool call '" + tool.name() + "' was interrupted.";
    }
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
