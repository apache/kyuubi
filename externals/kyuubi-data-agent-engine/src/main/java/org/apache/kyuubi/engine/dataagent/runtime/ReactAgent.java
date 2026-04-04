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

package org.apache.kyuubi.engine.dataagent.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.client.OpenAIClient;
import com.openai.core.http.StreamResponse;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionChunk;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessageFunctionToolCall;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionStreamOptions;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentError;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentFinish;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentStart;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentComplete;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentDelta;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepEnd;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepStart;
import org.apache.kyuubi.engine.dataagent.runtime.event.ToolCall;
import org.apache.kyuubi.engine.dataagent.runtime.event.ToolResult;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.AgentMiddleware;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReAct (Reasoning + Acting) agent loop using the OpenAI official Java SDK. Iterates through LLM
 * reasoning, tool execution, and result verification until the agent produces a final answer or
 * hits the iteration limit.
 *
 * <p>Emits {@link AgentEvent}s via the provided consumer for real-time token-level streaming.
 */
public class ReactAgent implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ReactAgent.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final OpenAIClient client;
  private final String defaultModelName;
  private final ToolRegistry toolRegistry;
  private final List<AgentMiddleware> middlewares;
  private final int maxIterations;
  private final String systemPrompt;
  private final long toolTimeoutSeconds;
  private final ExecutorService toolExecutor;

  /** Default tool execution timeout: 5 minutes. */
  private static final long DEFAULT_TOOL_TIMEOUT_SECONDS = 300;

  public ReactAgent(
      OpenAIClient client,
      String modelName,
      ToolRegistry toolRegistry,
      List<AgentMiddleware> middlewares,
      int maxIterations,
      String systemPrompt,
      long toolTimeoutSeconds) {
    this.client = client;
    this.defaultModelName = modelName;
    this.toolRegistry = toolRegistry;
    this.middlewares = middlewares != null ? middlewares : Collections.emptyList();
    this.maxIterations = maxIterations;
    this.systemPrompt = systemPrompt;
    this.toolTimeoutSeconds =
        toolTimeoutSeconds > 0 ? toolTimeoutSeconds : DEFAULT_TOOL_TIMEOUT_SECONDS;
    // Plain Java thread pool — intentionally not using Kyuubi's Scala ThreadUtils because this
    // module is pure Java to keep the dependency footprint minimal.
    AtomicInteger threadCount = new AtomicInteger();
    this.toolExecutor =
        new ThreadPoolExecutor(
            0,
            maxIterations,
            60L,
            TimeUnit.SECONDS,
            new java.util.concurrent.SynchronousQueue<>(),
            r -> {
              Thread t = new Thread(r, "agent-tool-executor-" + threadCount.getAndIncrement());
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Run the ReAct loop for the given request.
   *
   * @param request user-facing parameters (question, model override, etc.)
   * @param memory the conversation memory (may contain prior context)
   * @param eventConsumer callback for each agent event (token-level streaming)
   */
  public void run(
      AgentRunRequest request, ConversationMemory memory, Consumer<AgentEvent> eventConsumer) {
    String userInput = request.getUserInput();
    ApprovalMode approvalMode = request.getApprovalMode();
    String modelNameOverride = request.getModelName();

    String effectiveModel =
        (modelNameOverride != null && !modelNameOverride.isEmpty())
            ? modelNameOverride
            : defaultModelName;

    // System prompt is immutable for the lifetime of this agent — only set it on first run
    // to avoid redundant overwrites on multi-turn conversations.
    if (memory.getSystemPrompt() == null) {
      memory.setSystemPrompt(systemPrompt);
    }
    memory.addUserMessage(userInput);

    AgentContext ctx = new AgentContext(memory, approvalMode);
    ctx.setEventEmitter(event -> emit(ctx, event, eventConsumer));
    dispatchAgentStart(ctx);
    emit(ctx, new AgentStart(), eventConsumer);

    try {
      for (int step = 1; step <= maxIterations; step++) {
        ctx.setIteration(step);
        emit(ctx, new StepStart(step), eventConsumer);

        // 1. Build messages from memory
        List<ChatCompletionMessageParam> messages = memory.buildLlmMessages();

        // 2. Dispatch before_llm_call middleware — may skip or modify messages
        AgentMiddleware.LlmCallAction llmAction = dispatchBeforeLlmCall(ctx, messages);
        if (llmAction instanceof AgentMiddleware.LlmSkip) {
          emit(
              ctx,
              new AgentError(
                  "LLM call skipped by middleware: "
                      + ((AgentMiddleware.LlmSkip) llmAction).reason()),
              eventConsumer);
          emitFinish(ctx, eventConsumer);
          return;
        }
        if (llmAction instanceof AgentMiddleware.LlmModifyMessages) {
          messages = ((AgentMiddleware.LlmModifyMessages) llmAction).messages();
        }

        // 3. Stream LLM response with token-level events
        StreamResult result = streamLlmResponse(ctx, messages, effectiveModel, eventConsumer);
        if (result == null || result.isEmpty()) {
          emit(ctx, new AgentError("LLM returned empty response"), eventConsumer);
          emitFinish(ctx, eventConsumer);
          return;
        }

        // 4. Extract content and tool calls
        String content = result.content;
        List<ChatCompletionMessageToolCall> toolCalls = result.toolCalls;

        // 5. Emit ContentComplete (skip if content is empty, e.g. tool-call-only responses)
        if (!content.isEmpty()) {
          emit(ctx, new ContentComplete(content), eventConsumer);
        }

        // 6. Build assistant message and add to memory
        ChatCompletionAssistantMessageParam.Builder assistantBuilder =
            ChatCompletionAssistantMessageParam.builder();
        if (!content.isEmpty()) {
          assistantBuilder.content(content);
        }
        if (toolCalls != null && !toolCalls.isEmpty()) {
          assistantBuilder.toolCalls(toolCalls);
        }

        ChatCompletionAssistantMessageParam assistantMsg = assistantBuilder.build();
        memory.addAssistantMessage(assistantMsg);

        // 7. Dispatch after_llm_call middleware
        dispatchAfterLlmCall(ctx, assistantMsg);

        // 8. Check for tool calls — execute in 3 phases for parallelism
        if (toolCalls != null && !toolCalls.isEmpty()) {
          // Phase 1 (serial): middleware check + emit ToolCall events, collect approved calls
          List<ToolCallEntry> approved = new ArrayList<>();
          for (ChatCompletionMessageToolCall toolCall : toolCalls) {
            ChatCompletionMessageFunctionToolCall fnCall = toolCall.asFunction();
            String toolName = fnCall.function().name();
            Map<String, Object> toolArgs = parseToolArgs(fnCall.function().arguments());

            AgentMiddleware.ToolCallDenial denial =
                dispatchBeforeToolCall(ctx, fnCall.id(), toolName, toolArgs);
            if (denial != null) {
              String denied = "Tool call denied: " + denial.reason();
              memory.addToolResult(fnCall.id(), denied);
              emit(ctx, new ToolResult(fnCall.id(), toolName, denied, true), eventConsumer);
              continue;
            }

            emit(ctx, new ToolCall(fnCall.id(), toolName, toolArgs), eventConsumer);
            approved.add(new ToolCallEntry(fnCall, toolName, toolArgs));
          }

          // Phase 2 (concurrent): execute all approved tools in parallel
          List<CompletableFuture<String>> futures = new ArrayList<>(approved.size());
          for (ToolCallEntry entry : approved) {
            futures.add(
                CompletableFuture.supplyAsync(
                    () -> executeTool(entry.toolName, entry.fnCall.function()), toolExecutor));
          }

          // Phase 3 (serial): collect results in order, run afterToolCall, add to memory
          for (int i = 0; i < approved.size(); i++) {
            ToolCallEntry entry = approved.get(i);
            String toolOutput;
            try {
              toolOutput = futures.get(i).get(toolTimeoutSeconds, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
              futures.get(i).cancel(true);
              toolOutput = "Tool execution timed out after " + toolTimeoutSeconds + "s";
            } catch (ExecutionException e) {
              Throwable cause = e.getCause() != null ? e.getCause() : e;
              toolOutput = "Tool execution error: " + cause.getMessage();
            } catch (Exception e) {
              toolOutput = "Tool execution error: " + e.getMessage();
            }

            String modifiedResult =
                dispatchAfterToolCall(ctx, entry.toolName, entry.toolArgs, toolOutput);
            if (modifiedResult != null) {
              toolOutput = modifiedResult;
            }

            memory.addToolResult(entry.fnCall.id(), toolOutput);
            emit(
                ctx,
                new ToolResult(entry.fnCall.id(), entry.toolName, toolOutput, false),
                eventConsumer);
          }
          emit(ctx, new StepEnd(step), eventConsumer);
          continue;
        }

        // 9. No tool calls — agent finished
        emit(ctx, new StepEnd(step), eventConsumer);
        emit(
            ctx,
            new AgentFinish(
                step, ctx.getPromptTokens(), ctx.getCompletionTokens(), ctx.getTotalTokens()),
            eventConsumer);
        return;
      }

      // Hit max iterations
      emit(
          ctx, new AgentError("Reached maximum iterations (" + maxIterations + ")"), eventConsumer);
      emitFinish(ctx, eventConsumer);

    } finally {
      dispatchAgentFinish(ctx);
    }
  }

  /** Result of a streaming LLM call, assembled from chunks. */
  private static class StreamResult {
    final String content;
    final List<ChatCompletionMessageToolCall> toolCalls;

    StreamResult(String content, List<ChatCompletionMessageToolCall> toolCalls) {
      this.content = content;
      this.toolCalls = toolCalls;
    }

    boolean hasToolCalls() {
      return toolCalls != null && !toolCalls.isEmpty();
    }

    boolean isEmpty() {
      return (content == null || content.isEmpty()) && !hasToolCalls();
    }
  }

  /** Holds an approved tool call's parsed metadata for the 3-phase execution pipeline. */
  private static class ToolCallEntry {
    final ChatCompletionMessageFunctionToolCall fnCall;
    final String toolName;
    final Map<String, Object> toolArgs;

    ToolCallEntry(
        ChatCompletionMessageFunctionToolCall fnCall,
        String toolName,
        Map<String, Object> toolArgs) {
      this.fnCall = fnCall;
      this.toolName = toolName;
      this.toolArgs = toolArgs;
    }
  }

  /**
   * Stream LLM response, emitting ContentDelta for each text chunk. Assembles tool calls directly
   * from streamed chunks — no non-streaming fallback. Returns null only on error.
   */
  private StreamResult streamLlmResponse(
      AgentContext ctx,
      List<ChatCompletionMessageParam> messages,
      String effectiveModel,
      Consumer<AgentEvent> eventConsumer) {

    try {
      ChatCompletionCreateParams.Builder paramsBuilder =
          ChatCompletionCreateParams.builder()
              .model(effectiveModel)
              .streamOptions(ChatCompletionStreamOptions.builder().includeUsage(true).build());

      for (ChatCompletionMessageParam msg : messages) {
        paramsBuilder.addMessage(msg);
      }

      if (!toolRegistry.isEmpty()) {
        toolRegistry.addToolsTo(paramsBuilder);
      }

      StringBuilder contentAccumulator = new StringBuilder();
      // Tool call accumulators keyed by index
      Map<Integer, String> toolCallIds = new HashMap<>();
      Map<Integer, String> toolCallNames = new HashMap<>();
      Map<Integer, StringBuilder> toolCallArgs = new HashMap<>();

      try (StreamResponse<ChatCompletionChunk> stream =
          client.chat().completions().createStreaming(paramsBuilder.build())) {
        stream.stream()
            .forEach(
                chunk -> {
                  // Extract usage from the final chunk
                  chunk
                      .usage()
                      .ifPresent(
                          u ->
                              ctx.addTokenUsage(
                                  u.promptTokens(), u.completionTokens(), u.totalTokens()));

                  for (ChatCompletionChunk.Choice c : chunk.choices()) {
                    // Accumulate text content
                    c.delta()
                        .content()
                        .ifPresent(
                            text -> {
                              contentAccumulator.append(text);
                              emit(ctx, new ContentDelta(text), eventConsumer);
                            });

                    // Accumulate tool calls from deltas
                    c.delta()
                        .toolCalls()
                        .ifPresent(
                            tcs -> {
                              for (ChatCompletionChunk.Choice.Delta.ToolCall tc : tcs) {
                                int idx = (int) tc.index();
                                tc.id().ifPresent(id -> toolCallIds.put(idx, id));
                                tc.function()
                                    .ifPresent(
                                        fn -> {
                                          fn.name().ifPresent(name -> toolCallNames.put(idx, name));
                                          fn.arguments()
                                              .ifPresent(
                                                  args ->
                                                      toolCallArgs
                                                          .computeIfAbsent(
                                                              idx, k -> new StringBuilder())
                                                          .append(args));
                                        });
                              }
                            });
                  }
                });
      }

      // Assemble tool calls from accumulated chunks
      List<ChatCompletionMessageToolCall> toolCalls = null;
      if (!toolCallIds.isEmpty()) {
        toolCalls = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : toolCallIds.entrySet()) {
          int idx = entry.getKey();
          String id = entry.getValue();
          if (id == null || id.isEmpty()) {
            id =
                "local_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 24);
          }
          toolCalls.add(
              ChatCompletionMessageToolCall.ofFunction(
                  ChatCompletionMessageFunctionToolCall.builder()
                      .id(id)
                      .function(
                          ChatCompletionMessageFunctionToolCall.Function.builder()
                              .name(toolCallNames.getOrDefault(idx, ""))
                              .arguments(
                                  toolCallArgs.containsKey(idx)
                                      ? toolCallArgs.get(idx).toString()
                                      : "{}")
                              .build())
                      .build()));
        }
      }

      return new StreamResult(contentAccumulator.toString(), toolCalls);

    } catch (Exception e) {
      LOG.error("LLM streaming error", e);
      emit(ctx, new AgentError("LLM error: " + e.getMessage()), eventConsumer);
      return null;
    }
  }

  private String executeTool(
      String toolName, ChatCompletionMessageFunctionToolCall.Function function) {
    return toolRegistry.executeTool(toolName, function.arguments());
  }

  private static Map<String, Object> parseToolArgs(String json) {
    if (json == null || json.isEmpty()) {
      return new HashMap<>();
    }
    try {
      return JSON.readValue(json, new TypeReference<Map<String, Object>>() {});
    } catch (Exception e) {
      LOG.warn("Failed to parse tool arguments: {}", json, e);
      Map<String, Object> fallback = new HashMap<>();
      fallback.put("raw", json);
      return fallback;
    }
  }

  @Override
  public void close() {
    toolExecutor.shutdownNow();
    try {
      if (!toolExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Tool executor did not terminate within 10s timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // --- Middleware dispatch methods ---

  private void emitFinish(AgentContext ctx, Consumer<AgentEvent> eventConsumer) {
    emit(
        ctx,
        new AgentFinish(
            ctx.getIteration(),
            ctx.getPromptTokens(),
            ctx.getCompletionTokens(),
            ctx.getTotalTokens()),
        eventConsumer);
  }

  private void emit(AgentContext ctx, AgentEvent event, Consumer<AgentEvent> consumer) {
    AgentEvent filtered = event;
    for (AgentMiddleware mw : middlewares) {
      try {
        filtered = mw.onEvent(ctx, filtered);
        if (filtered == null) return;
      } catch (Exception e) {
        LOG.warn("Middleware onEvent error", e);
      }
    }
    consumer.accept(filtered);
  }

  private void dispatchAgentStart(AgentContext ctx) {
    for (AgentMiddleware mw : middlewares) {
      try {
        mw.onAgentStart(ctx);
      } catch (Exception e) {
        LOG.warn("Middleware onAgentStart error", e);
      }
    }
  }

  private void dispatchAgentFinish(AgentContext ctx) {
    for (int i = middlewares.size() - 1; i >= 0; i--) {
      try {
        middlewares.get(i).onAgentFinish(ctx);
      } catch (Exception e) {
        LOG.warn("Middleware onAgentFinish error", e);
      }
    }
  }

  private AgentMiddleware.LlmCallAction dispatchBeforeLlmCall(
      AgentContext ctx, List<ChatCompletionMessageParam> messages) {
    for (AgentMiddleware mw : middlewares) {
      try {
        AgentMiddleware.LlmCallAction action = mw.beforeLlmCall(ctx, messages);
        if (action != null) {
          return action;
        }
      } catch (Exception e) {
        LOG.warn("Middleware beforeLlmCall error", e);
      }
    }
    return null;
  }

  private void dispatchAfterLlmCall(
      AgentContext ctx, ChatCompletionAssistantMessageParam response) {
    for (int i = middlewares.size() - 1; i >= 0; i--) {
      try {
        middlewares.get(i).afterLlmCall(ctx, response);
      } catch (Exception e) {
        LOG.warn("Middleware afterLlmCall error", e);
      }
    }
  }

  private AgentMiddleware.ToolCallDenial dispatchBeforeToolCall(
      AgentContext ctx, String toolCallId, String toolName, Map<String, Object> toolArgs) {
    for (AgentMiddleware mw : middlewares) {
      try {
        AgentMiddleware.ToolCallDenial denial =
            mw.beforeToolCall(ctx, toolCallId, toolName, toolArgs);
        if (denial != null) return denial;
      } catch (Exception e) {
        LOG.error("Middleware beforeToolCall error, denying tool call as safe default", e);
        return new AgentMiddleware.ToolCallDenial("Middleware error: " + e.getMessage());
      }
    }
    return null;
  }

  private String dispatchAfterToolCall(
      AgentContext ctx, String toolName, Map<String, Object> toolArgs, String result) {
    String modified = null;
    for (int i = middlewares.size() - 1; i >= 0; i--) {
      try {
        String mwResult =
            middlewares
                .get(i)
                .afterToolCall(ctx, toolName, toolArgs, modified != null ? modified : result);
        if (mwResult != null) {
          modified = mwResult;
        }
      } catch (Exception e) {
        LOG.warn("Middleware afterToolCall error", e);
      }
    }
    return modified;
  }

  // --- Builder ---

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private OpenAIClient client;
    private String modelName;
    private ToolRegistry toolRegistry = new ToolRegistry();
    private final List<AgentMiddleware> middlewares = new ArrayList<>();
    private int maxIterations = 20;
    private String systemPrompt;
    private long toolTimeoutSeconds = DEFAULT_TOOL_TIMEOUT_SECONDS;

    public Builder client(OpenAIClient client) {
      this.client = client;
      return this;
    }

    public Builder modelName(String modelName) {
      this.modelName = modelName;
      return this;
    }

    public Builder toolRegistry(ToolRegistry toolRegistry) {
      this.toolRegistry = toolRegistry;
      return this;
    }

    public Builder addMiddleware(AgentMiddleware middleware) {
      this.middlewares.add(middleware);
      return this;
    }

    public Builder maxIterations(int maxIterations) {
      if (maxIterations < 1) {
        throw new IllegalArgumentException("maxIterations must be >= 1, got " + maxIterations);
      }
      this.maxIterations = maxIterations;
      return this;
    }

    public Builder systemPrompt(String systemPrompt) {
      this.systemPrompt = systemPrompt;
      return this;
    }

    public Builder toolTimeoutSeconds(long toolTimeoutSeconds) {
      this.toolTimeoutSeconds = toolTimeoutSeconds;
      return this;
    }

    public ReactAgent build() {
      if (client == null) throw new IllegalStateException("client is required");
      if (modelName == null) throw new IllegalStateException("modelName is required");
      if (toolRegistry == null) throw new IllegalStateException("toolRegistry is required");
      return new ReactAgent(
          client,
          modelName,
          toolRegistry,
          middlewares,
          maxIterations,
          systemPrompt,
          toolTimeoutSeconds);
    }
  }
}
