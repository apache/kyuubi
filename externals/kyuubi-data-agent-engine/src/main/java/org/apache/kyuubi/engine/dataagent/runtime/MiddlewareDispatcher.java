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

import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import java.util.List;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.AgentMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ApprovalMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.Decision;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ToolInvocation;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composite {@link AgentMiddleware} — folds a list of middlewares into one. Hook ordering follows
 * the onion model: {@code before*} / {@code on*Start} run first-to-last, {@code after*} / {@code
 * on*Finish} run last-to-first.
 *
 * <p>Component middlewares are internal framework code. If one throws during ordinary hook
 * dispatch, the agent run fails via {@link ReactAgent#run}; lifecycle cleanup hooks ({@link
 * #onAgentFinish}, {@link #onSessionClose}, {@link #onStop}) swallow exceptions so later
 * middlewares still get a chance to release state.
 */
final class MiddlewareDispatcher implements AgentMiddleware {

  private static final Logger LOG = LoggerFactory.getLogger(MiddlewareDispatcher.class);

  private final List<AgentMiddleware> middlewares;
  private final ApprovalMiddleware approvalMiddleware;

  MiddlewareDispatcher(List<AgentMiddleware> middlewares) {
    this.middlewares = middlewares;
    this.approvalMiddleware = findApprovalMiddleware(middlewares);
  }

  /**
   * Resolve a pending approval request. Not part of {@link AgentMiddleware} — special accessor for
   * the approval flow.
   */
  boolean resolveApproval(String requestId, boolean approved) {
    if (approvalMiddleware == null) return false;
    return approvalMiddleware.resolve(requestId, approved);
  }

  @Override
  public void onRegister(ToolRegistry registry) {
    for (AgentMiddleware mw : middlewares) {
      mw.onRegister(registry);
    }
  }

  @Override
  public void onAgentStart(AgentRunContext ctx) {
    for (AgentMiddleware mw : middlewares) {
      mw.onAgentStart(ctx);
    }
  }

  @Override
  public void onAgentFinish(AgentRunContext ctx) {
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

  @Override
  public void onSessionClose(String sessionId) {
    for (AgentMiddleware mw : middlewares) {
      try {
        mw.onSessionClose(sessionId);
      } catch (Exception e) {
        LOG.warn("Middleware onSessionClose error", e);
      }
    }
  }

  @Override
  public void onStop() {
    for (AgentMiddleware mw : middlewares) {
      try {
        mw.onStop();
      } catch (Exception e) {
        LOG.warn("Middleware onStop error", e);
      }
    }
  }

  /**
   * Fold {@code onEvent} in onion order. Returns PROCEED if untouched, REPLACE with the final event
   * if any middleware rewrote it, or ABORT if any short-circuited.
   */
  @Override
  public Decision<AgentEvent> onEvent(AgentRunContext ctx, AgentEvent event) {
    AgentEvent current = event;
    for (AgentMiddleware mw : middlewares) {
      Decision<AgentEvent> d = mw.onEvent(ctx, current);
      if (d.kind() == Decision.Kind.ABORT) return d;
      if (d.kind() == Decision.Kind.REPLACE) current = d.replacement();
    }
    return Decision.of(event, current);
  }

  /**
   * Fold {@code beforeLlmCall} in onion order so later middlewares see rewritten messages. Returns
   * PROCEED if untouched, REPLACE with the final value if any did, or ABORT if any short-circuited.
   */
  @Override
  public Decision<List<ChatCompletionMessageParam>> beforeLlmCall(
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
   * Fold {@code afterLlmCall} in reverse onion order so earlier middlewares see rewritten
   * responses. Returns the final response, or ABORT if any middleware short-circuits.
   */
  @Override
  public Decision<ChatCompletionAssistantMessageParam> afterLlmCall(
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
   * Fold {@code beforeToolCall} in onion order so later middlewares can further rewrite. Returns
   * PROCEED if untouched, REPLACE with the final invocation otherwise, or ABORT if any middleware
   * denies the call.
   */
  @Override
  public Decision<ToolInvocation> beforeToolCall(AgentRunContext ctx, ToolInvocation call) {
    ToolInvocation current = call;
    for (AgentMiddleware mw : middlewares) {
      Decision<ToolInvocation> d = mw.beforeToolCall(ctx, current);
      if (d.kind() == Decision.Kind.ABORT) return d;
      if (d.kind() == Decision.Kind.REPLACE) current = d.replacement();
    }
    return Decision.of(call, current);
  }

  /**
   * Fold {@code afterToolCall} in reverse onion order so earlier middlewares see rewritten results.
   * Returns the final result, or ABORT if any middleware short-circuits — caller decides how to
   * surface the abort (typically: use {@code reason()} as the result text the LLM sees).
   */
  @Override
  public Decision<String> afterToolCall(AgentRunContext ctx, ToolInvocation call, String result) {
    String current = result;
    for (int i = middlewares.size() - 1; i >= 0; i--) {
      Decision<String> d = middlewares.get(i).afterToolCall(ctx, call, current);
      if (d.kind() == Decision.Kind.ABORT) return d;
      if (d.kind() == Decision.Kind.REPLACE) current = d.replacement();
    }
    return Decision.of(result, current);
  }

  private static ApprovalMiddleware findApprovalMiddleware(List<AgentMiddleware> middlewares) {
    for (AgentMiddleware mw : middlewares) {
      if (mw instanceof ApprovalMiddleware) return (ApprovalMiddleware) mw;
    }
    return null;
  }
}
