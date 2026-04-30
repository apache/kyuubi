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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ApprovalMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.Decision;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ToolInvocation;
import org.apache.kyuubi.engine.dataagent.tool.ToolContext;
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
public class ReactAgent {

  private static final Logger LOG = LoggerFactory.getLogger(ReactAgent.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final OpenAIClient client;
  private final String defaultModelName;
  private final ToolRegistry toolRegistry;
  private final List<AgentMiddleware> middlewares;
  private final ApprovalMiddleware approvalMiddleware;
  private final int maxIterations;
  private final String systemPrompt;

  public ReactAgent(
      OpenAIClient client,
      String modelName,
      ToolRegistry toolRegistry,
      List<AgentMiddleware> middlewares,
      int maxIterations,
      String systemPrompt) {
    this.client = client;
    this.defaultModelName = modelName;
    this.toolRegistry = toolRegistry;
    this.middlewares = middlewares;
    this.approvalMiddleware = findApprovalMiddleware(middlewares);
    this.maxIterations = maxIterations;
    this.systemPrompt = systemPrompt;
  }

  private static ApprovalMiddleware findApprovalMiddleware(List<AgentMiddleware> middlewares) {
    for (AgentMiddleware mw : middlewares) {
      if (mw instanceof ApprovalMiddleware) return (ApprovalMiddleware) mw;
    }
    return null;
  }

  /** Resolve a pending approval request. Returns false if no pending request matches. */
  public boolean resolveApproval(String requestId, boolean approved) {
    if (approvalMiddleware == null) return false;
    return approvalMiddleware.resolve(requestId, approved);
  }

  /** Fan out session-close to every middleware. Errors in one middleware don't block others. */
  public void closeSession(String sessionId) {
    for (AgentMiddleware mw : middlewares) {
      try {
        mw.onSessionClose(sessionId);
      } catch (Exception e) {
        LOG.warn("Middleware onSessionClose error", e);
      }
    }
  }

  /** Fan out engine-stop to every middleware. Errors in one middleware don't block others. */
  public void stop() {
    for (AgentMiddleware mw : middlewares) {
      try {
        mw.onStop();
      } catch (Exception e) {
        LOG.warn("Middleware onStop error", e);
      }
    }
  }

  /**
   * Run the ReAct loop for the given request.
   *
   * @param request user-facing parameters (question, model override, etc.)
   * @param memory the conversation memory (may contain prior context)
   * @param eventConsumer callback for each agent event (token-level streaming)
   */
  public void run(
      AgentInvocation request, ConversationMemory memory, Consumer<AgentEvent> eventConsumer) {
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

    AgentRunContext ctx = new AgentRunContext(memory, approvalMode, request.getSessionId());
    ctx.setEventEmitter(event -> emit(ctx, event, eventConsumer));
    dispatchAgentStart(ctx);
    emit(ctx, new AgentStart(), eventConsumer);

    try {
      for (int step = 1; step <= maxIterations; step++) {
        ctx.setIteration(step);
        emit(ctx, new StepStart(step), eventConsumer);

        List<ChatCompletionMessageParam> messages =
            resolveMessagesForCall(ctx, memory.buildLlmMessages(), eventConsumer);
        if (messages == null) {
          // Middleware asked us to skip — AgentError + AgentFinish have already been emitted.
          return;
        }

        StreamResult result = streamLlmResponse(ctx, messages, effectiveModel, eventConsumer);
        if (result.isEmpty()) {
          emit(ctx, new AgentError("LLM returned empty response"), eventConsumer);
          emitFinish(ctx, eventConsumer);
          return;
        }

        if (!result.content.isEmpty()) {
          emit(ctx, new ContentComplete(result.content), eventConsumer);
        }
        ChatCompletionAssistantMessageParam assistantMsg = buildAssistantMessage(result);
        Decision<ChatCompletionAssistantMessageParam> after =
            dispatchAfterLlmCall(ctx, assistantMsg);
        if (after.kind() == Decision.Kind.ABORT) {
          emit(
              ctx,
              new AgentError("LLM response rejected by middleware: " + after.reason()),
              eventConsumer);
          emitFinish(ctx, eventConsumer);
          return;
        }
        if (after.kind() == Decision.Kind.REPLACE) assistantMsg = after.replacement();
        memory.addAssistantMessage(assistantMsg);

        List<ChatCompletionMessageToolCall> toolCalls = assistantMsg.toolCalls().orElse(null);
        if (toolCalls == null || toolCalls.isEmpty()) {
          // No tool calls — agent is done.
          emit(ctx, new StepEnd(step), eventConsumer);
          emitFinish(ctx, eventConsumer);
          return;
        }

        executeToolCalls(ctx, memory, toolCalls, eventConsumer);
        emit(ctx, new StepEnd(step), eventConsumer);
      }

      emit(
          ctx, new AgentError("Reached maximum iterations (" + maxIterations + ")"), eventConsumer);
      emitFinish(ctx, eventConsumer);

    } catch (Exception e) {
      LOG.error("Agent run failed", e);
      emit(
          ctx, new AgentError(e.getClass().getSimpleName() + ": " + e.getMessage()), eventConsumer);
      emitFinish(ctx, eventConsumer);
    } finally {
      dispatchAgentFinish(ctx);
    }
  }

  /**
   * Run {@code beforeLlmCall} middleware against {@code messages}. Returns the messages to send,
   * possibly rewritten by middleware, or {@code null} if middleware aborted the call (in which case
   * this method has already emitted the terminal events).
   */
  private List<ChatCompletionMessageParam> resolveMessagesForCall(
      AgentRunContext ctx,
      List<ChatCompletionMessageParam> messages,
      Consumer<AgentEvent> eventConsumer) {
    Decision<List<ChatCompletionMessageParam>> decision = dispatchBeforeLlmCall(ctx, messages);
    if (decision.kind() == Decision.Kind.ABORT) {
      emit(
          ctx,
          new AgentError("LLM call skipped by middleware: " + decision.reason()),
          eventConsumer);
      emitFinish(ctx, eventConsumer);
      return null;
    }
    return decision.kind() == Decision.Kind.REPLACE ? decision.replacement() : messages;
  }

  private static ChatCompletionAssistantMessageParam buildAssistantMessage(StreamResult result) {
    ChatCompletionAssistantMessageParam.Builder b = ChatCompletionAssistantMessageParam.builder();
    if (!result.content.isEmpty()) {
      b.content(result.content);
    }
    if (result.toolCalls != null && !result.toolCalls.isEmpty()) {
      b.toolCalls(result.toolCalls);
    }
    return b.build();
  }

  /**
   * Execute the assistant's tool calls in 3 phases:
   *
   * <ol>
   *   <li>Serial: run {@code beforeToolCall} middleware, emit {@link ToolCall} events, and collect
   *       the calls that survived approval.
   *   <li>Concurrent: fan out to {@link ToolRegistry#submitTool}, which always returns a future
   *       that completes normally — timeouts and execution errors surface as error strings.
   *   <li>Serial: join futures in order, run {@code afterToolCall}, and record results to memory.
   * </ol>
   */
  private void executeToolCalls(
      AgentRunContext ctx,
      ConversationMemory memory,
      List<ChatCompletionMessageToolCall> toolCalls,
      Consumer<AgentEvent> eventConsumer) {
    List<ToolCallEntry> approved = new ArrayList<>();
    for (ChatCompletionMessageToolCall toolCall : toolCalls) {
      ChatCompletionMessageFunctionToolCall fnCall = toolCall.asFunction();
      String toolName = fnCall.function().name();
      Map<String, Object> toolArgs;
      try {
        toolArgs = parseToolArgs(fnCall.function().arguments());
      } catch (IllegalArgumentException e) {
        // Malformed JSON from the LLM: record an error tool_result (preserves the
        // assistant/tool_result pairing the next API call needs) and let the loop self-correct.
        String err = "Tool call failed: " + e.getMessage();
        memory.addToolResult(fnCall.id(), err);
        emit(ctx, new ToolResult(fnCall.id(), toolName, err, true), eventConsumer);
        continue;
      }

      ToolInvocation invocation = new ToolInvocation(fnCall.id(), toolName, toolArgs);
      Decision<ToolInvocation> decision = dispatchBeforeToolCall(ctx, invocation);
      if (decision.kind() == Decision.Kind.ABORT) {
        String denied = "Tool call denied: " + decision.reason();
        memory.addToolResult(fnCall.id(), denied);
        emit(ctx, new ToolResult(fnCall.id(), toolName, denied, true), eventConsumer);
        continue;
      }
      boolean rewritten = decision.kind() == Decision.Kind.REPLACE;
      ToolInvocation effective = rewritten ? decision.replacement() : invocation;

      emit(ctx, new ToolCall(effective.id(), effective.name(), effective.args()), eventConsumer);
      approved.add(new ToolCallEntry(fnCall, effective, rewritten));
    }

    ToolContext toolCtx = new ToolContext(ctx.getSessionId());
    List<CompletableFuture<String>> futures = new ArrayList<>(approved.size());
    for (ToolCallEntry entry : approved) {
      futures.add(toolRegistry.submitTool(entry.invocation.name(), entry.argsJson(), toolCtx));
    }

    for (int i = 0; i < approved.size(); i++) {
      ToolCallEntry entry = approved.get(i);
      String raw = futures.get(i).join();
      String output = dispatchAfterToolCall(ctx, entry.invocation, raw);
      memory.addToolResult(entry.fnCall.id(), output);
      emit(
          ctx,
          new ToolResult(entry.fnCall.id(), entry.invocation.name(), output, false),
          eventConsumer);
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

    boolean isEmpty() {
      return content.isEmpty() && (toolCalls == null || toolCalls.isEmpty());
    }
  }

  /**
   * Holds an approved tool call's parsed metadata for the 3-phase execution pipeline. {@code
   * rewritten} is {@code true} when middleware replaced the {@link ToolInvocation}; in that case
   * args must be re-serialized for {@link ToolRegistry#submitTool}, otherwise the LLM's original
   * JSON is reused verbatim.
   */
  private static class ToolCallEntry {
    final ChatCompletionMessageFunctionToolCall fnCall;
    final ToolInvocation invocation;
    final boolean rewritten;

    ToolCallEntry(
        ChatCompletionMessageFunctionToolCall fnCall,
        ToolInvocation invocation,
        boolean rewritten) {
      this.fnCall = fnCall;
      this.invocation = invocation;
      this.rewritten = rewritten;
    }

    String argsJson() {
      if (!rewritten) return fnCall.function().arguments();
      try {
        return JSON.writeValueAsString(invocation.args());
      } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
        throw new IllegalStateException("Failed to serialize rewritten tool args", e);
      }
    }
  }

  /**
   * Stream LLM response, emitting ContentDelta for each text chunk. Assembles tool calls directly
   * from streamed chunks — no non-streaming fallback. Exceptions propagate to the top-level handler
   * in {@link #run}.
   */
  private StreamResult streamLlmResponse(
      AgentRunContext ctx,
      List<ChatCompletionMessageParam> messages,
      String effectiveModel,
      Consumer<AgentEvent> eventConsumer) {
    ChatCompletionCreateParams.Builder paramsBuilder =
        ChatCompletionCreateParams.builder()
            .model(effectiveModel)
            .streamOptions(ChatCompletionStreamOptions.builder().includeUsage(true).build());
    for (ChatCompletionMessageParam msg : messages) {
      paramsBuilder.addMessage(msg);
    }
    toolRegistry.addToolsTo(paramsBuilder);

    LOG.info("LLM request: model={}", effectiveModel);
    StreamAccumulator acc = new StreamAccumulator();
    try (StreamResponse<ChatCompletionChunk> stream =
        client.chat().completions().createStreaming(paramsBuilder.build())) {
      stream.stream().forEach(chunk -> consumeChunk(ctx, chunk, acc, eventConsumer));
    }
    return new StreamResult(acc.content.toString(), acc.buildToolCalls());
  }

  /** Fold one streaming chunk into {@code acc}, emitting per-token {@link ContentDelta}s. */
  private void consumeChunk(
      AgentRunContext ctx,
      ChatCompletionChunk chunk,
      StreamAccumulator acc,
      Consumer<AgentEvent> eventConsumer) {
    if (!acc.serverModelLogged) {
      LOG.info("LLM response: server-echoed model={}", chunk.model());
      acc.serverModelLogged = true;
    }
    chunk
        .usage()
        .ifPresent(u -> ctx.addTokenUsage(u.promptTokens(), u.completionTokens(), u.totalTokens()));

    for (ChatCompletionChunk.Choice c : chunk.choices()) {
      c.delta()
          .content()
          .ifPresent(
              text -> {
                acc.content.append(text);
                emit(ctx, new ContentDelta(text), eventConsumer);
              });
      c.delta().toolCalls().ifPresent(acc::mergeToolCallDeltas);
    }
  }

  /**
   * Mutable accumulator for a single streaming LLM turn. Tool call fields are keyed by the chunk's
   * {@code index} because provider SDKs may deliver a single logical call across multiple chunks
   * and only surface the {@code id}/{@code name} on the first one.
   */
  private static final class StreamAccumulator {
    final StringBuilder content = new StringBuilder();
    final Map<Integer, String> toolCallIds = new HashMap<>();
    final Map<Integer, String> toolCallNames = new HashMap<>();
    final Map<Integer, StringBuilder> toolCallArgs = new HashMap<>();
    boolean serverModelLogged = false;

    void mergeToolCallDeltas(List<ChatCompletionChunk.Choice.Delta.ToolCall> deltas) {
      for (ChatCompletionChunk.Choice.Delta.ToolCall tc : deltas) {
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
                                  .computeIfAbsent(idx, k -> new StringBuilder())
                                  .append(args));
                });
      }
    }

    /**
     * Materialize accumulated deltas into SDK tool-call objects. Returns {@code null} (not an empty
     * list) if no tool calls were seen, matching the existing {@link StreamResult} contract.
     */
    List<ChatCompletionMessageToolCall> buildToolCalls() {
      if (toolCallIds.isEmpty()) return null;
      List<ChatCompletionMessageToolCall> out = new ArrayList<>(toolCallIds.size());
      for (Map.Entry<Integer, String> e : toolCallIds.entrySet()) {
        int idx = e.getKey();
        String id = (e.getValue() == null || e.getValue().isEmpty()) ? synthId() : e.getValue();
        String args = toolCallArgs.containsKey(idx) ? toolCallArgs.get(idx).toString() : "{}";
        out.add(
            ChatCompletionMessageToolCall.ofFunction(
                ChatCompletionMessageFunctionToolCall.builder()
                    .id(id)
                    .function(
                        ChatCompletionMessageFunctionToolCall.Function.builder()
                            .name(toolCallNames.getOrDefault(idx, ""))
                            .arguments(args)
                            .build())
                    .build()));
      }
      return out;
    }

    /**
     * Synthesize an id for tool calls whose id never arrived on the stream (some OpenAI-compatible
     * providers omit it). The id has to be stable within a turn and unique across turns so the
     * assistant/tool_result pairing downstream holds.
     */
    private static String synthId() {
      return "local_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 24);
    }
  }

  private static Map<String, Object> parseToolArgs(String json) {
    if (json == null || json.isEmpty()) {
      return new HashMap<>();
    }
    try {
      return JSON.readValue(json, new TypeReference<Map<String, Object>>() {});
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException("Malformed tool-call arguments JSON: " + json, e);
    }
  }

  // --- Middleware dispatch methods ---
  //
  // Middlewares are internal framework code. If one throws, the agent run fails via the
  // top-level catch in run() — we do not wrap individual dispatch calls in try/catch.

  private void emitFinish(AgentRunContext ctx, Consumer<AgentEvent> eventConsumer) {
    emit(
        ctx,
        new AgentFinish(
            ctx.getIteration(),
            ctx.getPromptTokens(),
            ctx.getCompletionTokens(),
            ctx.getTotalTokens()),
        eventConsumer);
  }

  private void emit(AgentRunContext ctx, AgentEvent event, Consumer<AgentEvent> consumer) {
    AgentEvent filtered = event;
    for (AgentMiddleware mw : middlewares) {
      Decision<AgentEvent> d = mw.onEvent(ctx, filtered);
      if (d.kind() == Decision.Kind.ABORT) return;
      if (d.kind() == Decision.Kind.REPLACE) filtered = d.replacement();
    }
    consumer.accept(filtered);
  }

  private void dispatchAgentStart(AgentRunContext ctx) {
    for (AgentMiddleware mw : middlewares) {
      mw.onAgentStart(ctx);
    }
  }

  private void dispatchAgentFinish(AgentRunContext ctx) {
    // Runs even when the agent body threw, so swallow here to ensure every middleware's cleanup
    // gets a chance to run; otherwise we'd leak session state in later middlewares.
    for (int i = middlewares.size() - 1; i >= 0; i--) {
      try {
        middlewares.get(i).onAgentFinish(ctx);
      } catch (Exception e) {
        LOG.warn("Middleware onAgentFinish error", e);
      }
    }
  }

  /**
   * Run {@code beforeLlmCall} middleware in onion order, folding REPLACE so later middlewares see
   * rewritten messages. Returns PROCEED if no middleware touched the value, REPLACE with the final
   * value if any did, or ABORT if any middleware short-circuited.
   */
  private Decision<List<ChatCompletionMessageParam>> dispatchBeforeLlmCall(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
    List<ChatCompletionMessageParam> current = messages;
    for (AgentMiddleware mw : middlewares) {
      Decision<List<ChatCompletionMessageParam>> d = mw.beforeLlmCall(ctx, current);
      if (d.kind() == Decision.Kind.ABORT) return d;
      if (d.kind() == Decision.Kind.REPLACE) current = d.replacement();
    }
    return Decision.of(messages, current);
  }

  /**
   * Run {@code afterLlmCall} middleware in reverse onion order, folding REPLACE so earlier
   * middlewares see rewritten responses. Returns the final response, or ABORT if any middleware
   * short-circuits.
   */
  private Decision<ChatCompletionAssistantMessageParam> dispatchAfterLlmCall(
      AgentRunContext ctx, ChatCompletionAssistantMessageParam response) {
    ChatCompletionAssistantMessageParam current = response;
    for (int i = middlewares.size() - 1; i >= 0; i--) {
      Decision<ChatCompletionAssistantMessageParam> d =
          middlewares.get(i).afterLlmCall(ctx, current);
      if (d.kind() == Decision.Kind.ABORT) return d;
      if (d.kind() == Decision.Kind.REPLACE) current = d.replacement();
    }
    return Decision.of(response, current);
  }

  /**
   * Run {@code beforeToolCall} middleware in onion order, folding REPLACE so later middlewares can
   * further rewrite. Returns PROCEED if untouched, REPLACE with the final invocation otherwise, or
   * ABORT if any middleware denies the call.
   */
  private Decision<ToolInvocation> dispatchBeforeToolCall(
      AgentRunContext ctx, ToolInvocation call) {
    ToolInvocation current = call;
    for (AgentMiddleware mw : middlewares) {
      Decision<ToolInvocation> d = mw.beforeToolCall(ctx, current);
      if (d.kind() == Decision.Kind.ABORT) return d;
      if (d.kind() == Decision.Kind.REPLACE) current = d.replacement();
    }
    return Decision.of(call, current);
  }

  private String dispatchAfterToolCall(AgentRunContext ctx, ToolInvocation call, String result) {
    String current = result;
    for (int i = middlewares.size() - 1; i >= 0; i--) {
      Decision<String> d = middlewares.get(i).afterToolCall(ctx, call, current);
      if (d.kind() == Decision.Kind.REPLACE) {
        current = d.replacement();
      } else if (d.kind() == Decision.Kind.ABORT) {
        // afterToolCall.abort: discard result; reason replaces it so the LLM still sees something.
        current = d.reason();
      }
    }
    return current;
  }

  // --- Builder ---

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private OpenAIClient client;
    private String modelName;
    private ToolRegistry toolRegistry = new ToolRegistry(ToolRegistry.DEFAULT_TIMEOUT_SECONDS);
    private final List<AgentMiddleware> middlewares = new ArrayList<>();
    private int maxIterations = 20;
    private String systemPrompt;

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

    public ReactAgent build() {
      if (client == null) throw new IllegalStateException("client is required");
      if (modelName == null) throw new IllegalStateException("modelName is required");
      if (toolRegistry == null) throw new IllegalStateException("toolRegistry is required");
      for (AgentMiddleware mw : middlewares) {
        mw.onRegister(toolRegistry);
      }
      return new ReactAgent(
          client, modelName, toolRegistry, middlewares, maxIterations, systemPrompt);
    }
  }
}
