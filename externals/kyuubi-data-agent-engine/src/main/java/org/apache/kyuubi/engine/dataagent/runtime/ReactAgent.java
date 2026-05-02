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
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageFunctionToolCall;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
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
import org.apache.kyuubi.engine.dataagent.runtime.event.StepEnd;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepStart;
import org.apache.kyuubi.engine.dataagent.runtime.event.ToolCall;
import org.apache.kyuubi.engine.dataagent.runtime.event.ToolResult;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.AgentMiddleware;
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

  private final String defaultModelName;
  private final ToolRegistry toolRegistry;
  private final MiddlewareDispatcher dispatcher;
  private final LlmStreamClient llmStreamClient;
  private final int maxIterations;
  private final String systemPrompt;

  public ReactAgent(
      OpenAIClient client,
      String modelName,
      ToolRegistry toolRegistry,
      List<AgentMiddleware> middlewares,
      int maxIterations,
      String systemPrompt) {
    this.defaultModelName = modelName;
    this.toolRegistry = toolRegistry;
    this.dispatcher = new MiddlewareDispatcher(middlewares);
    this.llmStreamClient = new LlmStreamClient(client, toolRegistry);
    this.maxIterations = maxIterations;
    this.systemPrompt = systemPrompt;
  }

  /** Resolve a pending approval request. Returns false if no pending request matches. */
  public boolean resolveApproval(String requestId, boolean approved) {
    return dispatcher.resolveApproval(requestId, approved);
  }

  /** Fan out session-close to every middleware. Errors in one middleware don't block others. */
  public void closeSession(String sessionId) {
    dispatcher.onSessionClose(sessionId);
  }

  /** Fan out engine-stop to every middleware. Errors in one middleware don't block others. */
  public void stop() {
    dispatcher.onStop();
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
    // Wire the event pipeline: ctx.emit -> middleware.onEvent -> raw consumer.
    // Splitting filter and forward keeps the middleware composite ignorant of the consumer.
    ctx.setEventEmitter(
        event -> {
          Decision<AgentEvent> d = dispatcher.onEvent(ctx, event);
          if (d.kind() == Decision.Kind.ABORT) return;
          eventConsumer.accept(d.kind() == Decision.Kind.REPLACE ? d.replacement() : event);
        });
    dispatcher.onAgentStart(ctx);
    ctx.emit(new AgentStart());

    try {
      for (int step = 1; step <= maxIterations; step++) {
        ctx.setIteration(step);
        ctx.emit(new StepStart(step));

        List<ChatCompletionMessageParam> messages =
            resolveMessagesForCall(ctx, memory.buildLlmMessages());
        if (messages == null) {
          // Middleware asked us to skip — AgentError + AgentFinish have already been emitted.
          return;
        }

        LlmStreamClient.StreamResult result = llmStreamClient.stream(ctx, messages, effectiveModel);
        if (result.isEmpty()) {
          ctx.emit(new AgentError("LLM returned empty response"));
          emitFinish(ctx);
          return;
        }

        if (!result.content.isEmpty()) {
          ctx.emit(new ContentComplete(result.content));
        }
        ChatCompletionAssistantMessageParam assistantMsg = result.toAssistantMessage();
        Decision<ChatCompletionAssistantMessageParam> after =
            dispatcher.afterLlmCall(ctx, assistantMsg);
        if (after.kind() == Decision.Kind.ABORT) {
          ctx.emit(new AgentError("LLM response rejected by middleware: " + after.reason()));
          emitFinish(ctx);
          return;
        }
        if (after.kind() == Decision.Kind.REPLACE) assistantMsg = after.replacement();
        memory.addAssistantMessage(assistantMsg);

        List<ChatCompletionMessageToolCall> toolCalls = assistantMsg.toolCalls().orElse(null);
        if (toolCalls == null || toolCalls.isEmpty()) {
          // No tool calls — agent is done.
          ctx.emit(new StepEnd(step));
          emitFinish(ctx);
          return;
        }

        executeToolCalls(ctx, memory, toolCalls);
        ctx.emit(new StepEnd(step));
      }

      ctx.emit(new AgentError("Reached maximum iterations (" + maxIterations + ")"));
      emitFinish(ctx);

    } catch (Exception e) {
      LOG.error("Agent run failed", e);
      ctx.emit(new AgentError(e.getClass().getSimpleName() + ": " + e.getMessage()));
      emitFinish(ctx);
    } finally {
      dispatcher.onAgentFinish(ctx);
    }
  }

  private static void emitFinish(AgentRunContext ctx) {
    ctx.emit(
        new AgentFinish(
            ctx.getIteration(),
            ctx.getPromptTokens(),
            ctx.getCompletionTokens(),
            ctx.getTotalTokens()));
  }

  /**
   * Run {@code beforeLlmCall} middleware against {@code messages}. Returns the messages to send,
   * possibly rewritten by middleware, or {@code null} if middleware aborted the call (in which case
   * this method has already emitted the terminal events).
   */
  private List<ChatCompletionMessageParam> resolveMessagesForCall(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
    Decision<List<ChatCompletionMessageParam>> decision = dispatcher.beforeLlmCall(ctx, messages);
    if (decision.kind() == Decision.Kind.ABORT) {
      ctx.emit(new AgentError("LLM call skipped by middleware: " + decision.reason()));
      emitFinish(ctx);
      return null;
    }
    return decision.kind() == Decision.Kind.REPLACE ? decision.replacement() : messages;
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
      List<ChatCompletionMessageToolCall> toolCalls) {
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
        ctx.emit(new ToolResult(fnCall.id(), toolName, err, true));
        continue;
      }

      ToolInvocation invocation = new ToolInvocation(fnCall.id(), toolName, toolArgs);
      Decision<ToolInvocation> decision = dispatcher.beforeToolCall(ctx, invocation);
      if (decision.kind() == Decision.Kind.ABORT) {
        String denied = "Tool call denied: " + decision.reason();
        memory.addToolResult(fnCall.id(), denied);
        ctx.emit(new ToolResult(fnCall.id(), toolName, denied, true));
        continue;
      }
      boolean rewritten = decision.kind() == Decision.Kind.REPLACE;
      ToolInvocation effective = rewritten ? decision.replacement() : invocation;

      ctx.emit(new ToolCall(effective.id(), effective.name(), effective.args()));
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
      Decision<String> after = dispatcher.afterToolCall(ctx, entry.invocation, raw);
      // ABORT.afterToolCall: discard the result; surface reason() to the LLM and mark the event
      // as an error so listeners can distinguish it from a successful tool result.
      boolean isError = after.kind() == Decision.Kind.ABORT;
      String output =
          after.kind() == Decision.Kind.ABORT
              ? after.reason()
              : (after.kind() == Decision.Kind.REPLACE ? after.replacement() : raw);
      memory.addToolResult(entry.fnCall.id(), output);
      ctx.emit(new ToolResult(entry.fnCall.id(), entry.invocation.name(), output, isError));
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
