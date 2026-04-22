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
   * Called before each LLM invocation. Return non-null to skip or modify the LLM call. Runs
   * first-to-last.
   *
   * @return {@code null} to proceed normally, {@link LlmSkip} to abort, or {@link
   *     LlmModifyMessages} to replace the message list for this call.
   */
  default LlmCallAction beforeLlmCall(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
    return null;
  }

  /** Called after each LLM invocation. Runs last-to-first. */
  default void afterLlmCall(AgentRunContext ctx, ChatCompletionAssistantMessageParam response) {}

  /** Called before each tool execution. Return non-null to deny the call. Runs first-to-last. */
  default ToolCallDenial beforeToolCall(
      AgentRunContext ctx, String toolCallId, String toolName, Map<String, Object> toolArgs) {
    return null;
  }

  /**
   * Called after each tool execution. Runs last-to-first.
   *
   * <p>Returns {@code String} (not {@code void}) so that middlewares can intercept and transform
   * the tool result before it is fed back to the LLM — e.g. for data masking, output truncation, or
   * injecting metadata. Return {@code null} to keep the original result unchanged; return a
   * non-null value to replace it.
   */
  default String afterToolCall(
      AgentRunContext ctx, String toolName, Map<String, Object> toolArgs, String result) {
    return null;
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
   * Base type for {@code beforeLlmCall} return values. Subtypes: {@link LlmSkip} to abort the LLM
   * call, {@link LlmModifyMessages} to replace the message list for this call.
   */
  abstract class LlmCallAction {
    private LlmCallAction() {}
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

  /** Returned from {@code beforeToolCall} to deny a tool call. Non-null means denied. */
  class ToolCallDenial {
    private final String reason;

    public ToolCallDenial(String reason) {
      this.reason = reason;
    }

    public String reason() {
      return reason;
    }
  }
}
