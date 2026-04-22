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

import java.util.function.Consumer;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;

/**
 * Mutable context passed through the middleware pipeline and agent loop. Tracks the current state
 * of agent execution including iteration count, token usage, and custom middleware state.
 */
public class AgentRunContext {

  private final ConversationMemory memory;
  private final String sessionId;
  private Consumer<AgentEvent> eventEmitter;
  private int iteration;
  private long promptTokens;
  private long completionTokens;
  private long totalTokens;
  private ApprovalMode approvalMode;

  public AgentRunContext(ConversationMemory memory, ApprovalMode approvalMode) {
    this(memory, approvalMode, null);
  }

  public AgentRunContext(ConversationMemory memory, ApprovalMode approvalMode, String sessionId) {
    this.memory = memory;
    this.iteration = 0;
    this.approvalMode = approvalMode;
    this.sessionId = sessionId;
  }

  public ConversationMemory getMemory() {
    return memory;
  }

  /**
   * The upstream session identifier this run belongs to. Threaded down from {@code
   * DataAgentProvider.run(sessionId, ...)}. May be {@code null} in unit tests that do not exercise
   * session-scoped middleware.
   */
  public String getSessionId() {
    return sessionId;
  }

  public int getIteration() {
    return iteration;
  }

  public void setIteration(int iteration) {
    this.iteration = iteration;
  }

  public long getPromptTokens() {
    return promptTokens;
  }

  public long getCompletionTokens() {
    return completionTokens;
  }

  public long getTotalTokens() {
    return totalTokens;
  }

  /**
   * Record one LLM call's usage. Updates both the per-run counters on this context and the
   * session-level cumulative on the underlying {@link ConversationMemory}, so middlewares that need
   * a session-wide picture can read it directly from memory without keeping their own bookkeeping.
   */
  public void addTokenUsage(long prompt, long completion, long total) {
    this.promptTokens += prompt;
    this.completionTokens += completion;
    this.totalTokens += total;
    memory.addCumulativeTokens(prompt, completion, total);
  }

  public ApprovalMode getApprovalMode() {
    return approvalMode;
  }

  public void setApprovalMode(ApprovalMode approvalMode) {
    this.approvalMode = approvalMode;
  }

  public void setEventEmitter(Consumer<AgentEvent> eventEmitter) {
    this.eventEmitter = eventEmitter;
  }

  /** Emit an event through the agent's event pipeline. Available for use by middlewares. */
  public void emit(AgentEvent event) {
    if (eventEmitter != null) {
      eventEmitter.accept(event);
    }
  }
}
