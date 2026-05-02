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
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;

/**
 * Middleware interface for the Data Agent ReAct loop. Middlewares are executed in onion-model
 * order: before_* hooks run first-to-last, after_* hooks run last-to-first.
 *
 * <p>All hooks have default no-op implementations. Override only what you need. Every interceptor
 * hook returns {@link Decision} — see that class for the {@code proceed / replace / abort}
 * vocabulary and per-hook semantics of {@code abort}.
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

  /** Called before each LLM invocation. Runs first-to-last. */
  default Decision<List<ChatCompletionMessageParam>> beforeLlmCall(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
    return Decision.proceed();
  }

  /**
   * Called after each LLM invocation, before the assistant message is committed to memory or
   * inspected for tool calls. Runs last-to-first. Replacing the response lets middleware rewrite
   * what enters memory or strip tool calls before they execute.
   */
  default Decision<ChatCompletionAssistantMessageParam> afterLlmCall(
      AgentRunContext ctx, ChatCompletionAssistantMessageParam response) {
    return Decision.proceed();
  }

  /**
   * Called before each tool execution. Runs first-to-last. Replacing the {@link ToolInvocation}
   * lets middleware rewrite the tool name or args (e.g. inject a SQL LIMIT, redact parameters)
   * before execution.
   */
  default Decision<ToolInvocation> beforeToolCall(AgentRunContext ctx, ToolInvocation call) {
    return Decision.proceed();
  }

  /** Called after each tool execution. Runs last-to-first. */
  default Decision<String> afterToolCall(AgentRunContext ctx, ToolInvocation call, String result) {
    return Decision.proceed();
  }

  /** Called for every event before it is emitted. Runs first-to-last. */
  default Decision<AgentEvent> onEvent(AgentRunContext ctx, AgentEvent event) {
    return Decision.proceed();
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
}
