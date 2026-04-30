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

import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import java.util.List;
import java.util.Map;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;

/**
 * Middleware interface for the Data Agent ReAct loop. Middlewares are executed in onion-model
 * order: before_* hooks run first-to-last, after_* hooks run last-to-first.
 *
 * <p>All hooks have default no-op implementations. Override only what you need.
 */
public interface AgentMiddleware {

  /**
   * Called once when the middleware is wired into the agent. Register companion tools that are part
   * of the middleware's contract, or capture a reference to the registry for later use. Dispatched
   * by {@code ReactAgent.Builder.build} before the agent accepts any requests.
   */
  default void onRegister(ToolRegistry registry) {}

  /** Called when the agent starts processing a user query. Runs first-to-last. */
  default void onAgentStart(AgentRunContext ctx) {}

  /** Called when the agent finishes. Runs last-to-first (cleanup order). */
  default void onAgentFinish(AgentRunContext ctx) {}

  /**
   * Called before each LLM invocation. Runs first-to-last.
   *
   * @return {@link LlmNoopAction#INSTANCE} to proceed normally, {@link LlmSkip} to abort, or {@link
   *     LlmModifyMessages} to replace the message list for this call.
   */
  default LlmCallAction beforeLlmCall(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
    return LlmNoopAction.INSTANCE;
  }

  /** Called after each LLM invocation. Runs last-to-first. */
  default void afterLlmCall(AgentRunContext ctx, ChatCompletionAssistantMessageParam response) {}

  /**
   * Called before each tool execution. Runs first-to-last.
   *
   * @return {@link ToolCallApproval#INSTANCE} to allow the call, {@link ToolCallDenial} to block
   *     it.
   */
  default ToolCallAction beforeToolCall(
      AgentRunContext ctx, String toolCallId, String toolName, Map<String, Object> toolArgs) {
    return ToolCallApproval.INSTANCE;
  }

  /**
   * Called after each tool execution. Runs last-to-first.
   *
   * <p>Middlewares can intercept and transform the tool result before it is fed back to the LLM —
   * e.g. for data masking, output truncation, or injecting metadata.
   *
   * @return {@link ToolResultUnchanged#INSTANCE} to keep the original result, {@link
   *     ToolResultReplace} to substitute it.
   */
  default ToolResultAction afterToolCall(
      AgentRunContext ctx, String toolName, Map<String, Object> toolArgs, String result) {
    return ToolResultUnchanged.INSTANCE;
  }

  /**
   * Called for every event before it is emitted. Return null to suppress the event. Runs
   * first-to-last.
   */
  default AgentEvent onEvent(AgentRunContext ctx, AgentEvent event) {
    return event;
  }

  /**
   * Called when a session is closed. Clean up per-session state (scratch files, pending tasks,
   * counters). Idempotent. Dispatched by {@code ReactAgent.closeSession}.
   */
  default void onSessionClose(String sessionId) {}

  /**
   * Called when the engine is stopping. Release global resources and unblock any threads still
   * waiting on this middleware. Dispatched by {@code ReactAgent.stop}.
   */
  default void onStop() {}

  /**
   * Base type for {@code beforeLlmCall} return values. Subtypes: {@link LlmNoopAction} to proceed
   * normally, {@link LlmSkip} to abort the LLM call, {@link LlmModifyMessages} to replace the
   * message list for this call.
   */
  abstract class LlmCallAction {
    private LlmCallAction() {}
  }

  /** Returned from {@code beforeLlmCall} to proceed without changing the LLM call. */
  final class LlmNoopAction extends LlmCallAction {
    public static final LlmNoopAction INSTANCE = new LlmNoopAction();

    private LlmNoopAction() {}
  }

  /** Returned from {@code beforeLlmCall} to skip the LLM call and abort the agent loop. */
  class LlmSkip extends LlmCallAction {
    private final String reason;

    public LlmSkip(String reason) {
      this.reason = reason;
    }

    public String reason() {
      return reason;
    }
  }

  /**
   * Returned from {@code beforeLlmCall} to replace the message list for this LLM invocation. The
   * agent loop continues normally with the modified messages.
   */
  class LlmModifyMessages extends LlmCallAction {
    private final List<ChatCompletionMessageParam> messages;

    public LlmModifyMessages(List<ChatCompletionMessageParam> messages) {
      this.messages = messages;
    }

    public List<ChatCompletionMessageParam> messages() {
      return messages;
    }
  }

  /**
   * Base type for {@code beforeToolCall} return values. Subtypes: {@link ToolCallApproval} to allow
   * the call, {@link ToolCallDenial} to block it.
   */
  abstract class ToolCallAction {
    private ToolCallAction() {}
  }

  /** Returned from {@code beforeToolCall} to allow the tool call to proceed. */
  final class ToolCallApproval extends ToolCallAction {
    public static final ToolCallApproval INSTANCE = new ToolCallApproval();

    private ToolCallApproval() {}
  }

  /** Returned from {@code beforeToolCall} to block a tool call. */
  final class ToolCallDenial extends ToolCallAction {
    private final String reason;

    public ToolCallDenial(String reason) {
      this.reason = reason;
    }

    public String reason() {
      return reason;
    }
  }

  /**
   * Base type for {@code afterToolCall} return values. Subtypes: {@link ToolResultUnchanged} to
   * pass the result through, {@link ToolResultReplace} to substitute it.
   */
  abstract class ToolResultAction {
    private ToolResultAction() {}
  }

  /** Returned from {@code afterToolCall} to keep the tool result as is. */
  final class ToolResultUnchanged extends ToolResultAction {
    public static final ToolResultUnchanged INSTANCE = new ToolResultUnchanged();

    private ToolResultUnchanged() {}
  }

  /** Returned from {@code afterToolCall} to replace the tool result with a new string. */
  final class ToolResultReplace extends ToolResultAction {
    private final String replacement;

    public ToolResultReplace(String replacement) {
      this.replacement = replacement;
    }

    public String replacement() {
      return replacement;
    }
  }
}
