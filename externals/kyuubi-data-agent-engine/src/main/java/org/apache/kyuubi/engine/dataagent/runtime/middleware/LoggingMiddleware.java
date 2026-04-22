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
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentError;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepStart;
import org.apache.kyuubi.engine.dataagent.runtime.event.ToolResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Logging middleware that prints agent lifecycle events for debugging and observability.
 *
 * <p>Picks up {@code operationId} and {@code sessionId} from SLF4J MDC (set by ExecuteStatement) to
 * tag every log line with the Kyuubi operation/session context.
 *
 * <p>Log structure:
 *
 * <pre>
 *   [op:abcd1234] START user_input="..."
 *   [op:abcd1234] Step 1
 *   [op:abcd1234] LLM call: step=1, messages=3
 *   [op:abcd1234] LLM response: step=1, content="...(truncated)", tool_calls=1
 *   [op:abcd1234] Tool call: sql_query {sql=SELECT ...}
 *   [op:abcd1234] Tool result: sql_query -> "| col1 | col2 |...(truncated)"
 *   [op:abcd1234] FINISH steps=2, tokens=1234
 * </pre>
 */
public class LoggingMiddleware implements AgentMiddleware {

  private static final Logger LOG = LoggerFactory.getLogger("DataAgent");

  private static final int MAX_PREVIEW_LENGTH = 500;

  private static String prefix() {
    String sessionId = MDC.get("sessionId");
    String opId = MDC.get("operationId");
    StringBuilder sb = new StringBuilder();
    if (sessionId != null) {
      sb.append("[s:").append(shortId(sessionId)).append("]");
    }
    if (opId != null) {
      sb.append("[op:").append(shortId(opId)).append("]");
    }
    if (sb.length() > 0) {
      sb.append(" ");
    }
    return sb.toString();
  }

  /**
   * Take the first segment of a UUID (before the first dash). e.g. "327d8c5b-91ef-..." → "327d8c5b"
   */
  private static String shortId(String id) {
    int dash = id.indexOf('-');
    return dash > 0 ? id.substring(0, dash) : id;
  }

  @Override
  public void onAgentStart(AgentRunContext ctx) {
    LOG.debug("{}START user_input=\"{}\"", prefix(), truncate(ctx.getMemory().getLastUserInput()));
  }

  @Override
  public void onAgentFinish(AgentRunContext ctx) {
    LOG.info(
        "{}FINISH steps={}, prompt_tokens={}, completion_tokens={}, total_tokens={}",
        prefix(),
        ctx.getIteration(),
        ctx.getPromptTokens(),
        ctx.getCompletionTokens(),
        ctx.getTotalTokens());
  }

  @Override
  public LlmCallAction beforeLlmCall(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
    LOG.info("{}LLM call: step={}, messages={}", prefix(), ctx.getIteration(), messages.size());
    return null;
  }

  @Override
  public void afterLlmCall(AgentRunContext ctx, ChatCompletionAssistantMessageParam response) {
    String content = response.content().map(Object::toString).orElse("");
    int toolCallCount = response.toolCalls().map(List::size).orElse(0);
    LOG.info(
        "{}LLM response: step={}, content=\"{}\", tool_calls={}, "
            + "usage(cumulative): prompt={}, completion={}, total={}",
        prefix(),
        ctx.getIteration(),
        truncate(content),
        toolCallCount,
        ctx.getPromptTokens(),
        ctx.getCompletionTokens(),
        ctx.getTotalTokens());
  }

  @Override
  public ToolCallDenial beforeToolCall(
      AgentRunContext ctx, String toolCallId, String toolName, Map<String, Object> toolArgs) {
    LOG.info("{}Tool call: id={}, name={}", prefix(), toolCallId, toolName);
    LOG.debug("{}Tool args: {}", prefix(), toolArgs);
    return null;
  }

  @Override
  public String afterToolCall(
      AgentRunContext ctx, String toolName, Map<String, Object> toolArgs, String result) {
    LOG.info("{}Tool result: {} -> \"{}\"", prefix(), toolName, truncate(result));
    return null;
  }

  @Override
  public AgentEvent onEvent(AgentRunContext ctx, AgentEvent event) {
    switch (event.eventType()) {
      case STEP_START:
        LOG.info("{}Step {}", prefix(), ((StepStart) event).stepNumber());
        break;
      case ERROR:
        LOG.error("{}ERROR: {}", prefix(), ((AgentError) event).message());
        break;
      case TOOL_RESULT:
        ToolResult tr = (ToolResult) event;
        if (tr.isError()) {
          LOG.warn("{}Tool error: {} -> \"{}\"", prefix(), tr.toolName(), truncate(tr.output()));
        }
        break;
      default:
        break;
    }
    return event;
  }

  private static String truncate(String s) {
    if (s == null) return "";
    return s.length() <= MAX_PREVIEW_LENGTH ? s : s.substring(0, MAX_PREVIEW_LENGTH) + "...";
  }
}
