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
import com.openai.models.chat.completions.ChatCompletionSystemMessageParam;
import com.openai.models.chat.completions.ChatCompletionToolMessageParam;
import com.openai.models.chat.completions.ChatCompletionUserMessageParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Manages conversation history for a Data Agent session. Ensures tool result messages are never
 * orphaned from their corresponding AI messages.
 *
 * <p>Each instance is session-scoped and accessed sequentially within a single ReAct loop — no
 * synchronization is needed. Cross-session concurrency is handled by the provider's session map.
 */
public class ConversationMemory {

  /**
   * System prompt prepended to every LLM call built from this memory. Rebuilt by the provider on
   * each invocation (datasource/tool metadata can change between turns), so it lives outside the
   * {@link #messages} list rather than being inserted as the first entry. May be {@code null} until
   * the first {@link #setSystemPrompt} call — {@link #buildLlmMessages()} will simply omit the
   * system slot in that case.
   */
  private String systemPrompt;

  /**
   * The raw content of the most recent user message added via {@link #addUserMessage}. Cached
   * separately from {@link #messages} so middleware and callers can recover the current turn's
   * question after compaction rewrites history. Not used by the LLM call path itself.
   */
  private String lastUserInput;

  /**
   * The ordered conversation history: user / assistant / tool messages, in the order the LLM will
   * see them. The system prompt is intentionally NOT stored here (see {@link #systemPrompt}).
   * Mutated in place by {@link #replaceHistory} during compaction; otherwise append-only.
   */
  private final List<ChatCompletionMessageParam> messages = new ArrayList<>();

  /**
   * Session-level running total of {@code prompt_tokens} reported by every LLM call on this
   * conversation (across ReAct turns). Intended for billing, quota, and observability — not used by
   * any runtime decision. Updated via {@link #addCumulativeTokens}.
   */
  private long cumulativePromptTokens;

  /**
   * Session-level running total of {@code completion_tokens}. See {@link #cumulativePromptTokens}.
   */
  private long cumulativeCompletionTokens;

  /**
   * Session-level running total of {@code total_tokens} (prompt + completion as reported by the
   * provider — not necessarily the sum of the two counters above, since providers may count
   * cached/reasoning tokens differently). See {@link #cumulativePromptTokens}.
   */
  private long cumulativeTotalTokens;

  /**
   * The {@code total_tokens} reported by the single most recent LLM call, or {@code 0} if no call
   * has completed yet. Distinct from the cumulative counters: this is a snapshot, overwritten every
   * call. Used by {@link
   * org.apache.kyuubi.engine.dataagent.runtime.middleware.CompactionMiddleware} to estimate the
   * next prompt size (the last response becomes part of the next prompt, so the next call's prompt
   * is at least {@code lastTotalTokens}). Persists across ReAct turns until the next call
   * overwrites it.
   */
  private long lastTotalTokens;

  public ConversationMemory() {}

  public String getSystemPrompt() {
    return systemPrompt;
  }

  public void setSystemPrompt(String prompt) {
    this.systemPrompt = prompt;
  }

  public void addUserMessage(String content) {
    this.lastUserInput = content;
    messages.add(
        ChatCompletionMessageParam.ofUser(
            ChatCompletionUserMessageParam.builder().content(content).build()));
  }

  public String getLastUserInput() {
    return lastUserInput;
  }

  public void addAssistantMessage(ChatCompletionAssistantMessageParam message) {
    messages.add(ChatCompletionMessageParam.ofAssistant(message));
  }

  public void addToolResult(String toolCallId, String content) {
    messages.add(
        ChatCompletionMessageParam.ofTool(
            ChatCompletionToolMessageParam.builder()
                .toolCallId(toolCallId)
                .content(content)
                .build()));
  }

  /**
   * Build the full message list for LLM API invocation: [system prompt] + conversation history.
   *
   * <p>No windowing is applied — callers are responsible for managing context length (e.g. via a
   * token-based truncation strategy).
   *
   * @see #getHistory() for history-only access without system prompt
   */
  public List<ChatCompletionMessageParam> buildLlmMessages() {
    List<ChatCompletionMessageParam> result = new ArrayList<>(messages.size() + 1);
    if (systemPrompt != null) {
      result.add(
          ChatCompletionMessageParam.ofSystem(
              ChatCompletionSystemMessageParam.builder().content(systemPrompt).build()));
    }
    result.addAll(messages);
    return Collections.unmodifiableList(result);
  }

  /**
   * Returns the conversation history (user, assistant, tool messages) without the system prompt.
   * Useful for middleware that needs to inspect or compact history.
   */
  public List<ChatCompletionMessageParam> getHistory() {
    return Collections.unmodifiableList(new ArrayList<>(messages));
  }

  /**
   * Replace the conversation history with a compacted version. Useful for context-length management
   * strategies (e.g., summarizing older messages).
   *
   * <p>Also clears {@link #lastTotalTokens}: the prior snapshot referred to a prompt whose bulk we
   * just discarded, so it no longer describes anything in memory. Leaving it stale would keep the
   * compaction trigger armed until the next successful LLM call overwrites it — fine on the happy
   * path, but if that call fails the next turn would re-enter compaction against already-compacted
   * history. Zeroing means "unknown, wait for the next real usage report". Cumulative totals are
   * intentionally preserved (session-level accounting, must not regress on internal compaction).
   */
  public void replaceHistory(List<ChatCompletionMessageParam> compacted) {
    messages.clear();
    messages.addAll(compacted);
    this.lastTotalTokens = 0;
  }

  public void clear() {
    messages.clear();
  }

  public int size() {
    return messages.size();
  }

  public long getCumulativePromptTokens() {
    return cumulativePromptTokens;
  }

  public long getCumulativeCompletionTokens() {
    return cumulativeCompletionTokens;
  }

  public long getCumulativeTotalTokens() {
    return cumulativeTotalTokens;
  }

  public long getLastTotalTokens() {
    return lastTotalTokens;
  }

  /** Add one LLM call's usage to the session cumulative. Intended for {@link AgentRunContext}. */
  public void addCumulativeTokens(long prompt, long completion, long total) {
    this.cumulativePromptTokens += prompt;
    this.cumulativeCompletionTokens += completion;
    this.cumulativeTotalTokens += total;
    this.lastTotalTokens = total;
  }
}
