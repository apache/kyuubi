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

package org.apache.kyuubi.engine.dataagent.runtime.event;

/**
 * The agent has finished its analysis. Carries two distinct usage views: {@code accumulated*} sums
 * every LLM call in this run (billing / total cost), {@code last*} reflects only the final call
 * (current context size, last response size).
 */
public final class AgentFinish extends AgentEvent {
  private final int totalSteps;
  private final long accumulatedPromptTokens;
  private final long accumulatedCompletionTokens;
  private final long lastPromptTokens;
  private final long lastCompletionTokens;

  public AgentFinish(
      int totalSteps,
      long accumulatedPromptTokens,
      long accumulatedCompletionTokens,
      long lastPromptTokens,
      long lastCompletionTokens) {
    super(EventType.AGENT_FINISH);
    this.totalSteps = totalSteps;
    this.accumulatedPromptTokens = accumulatedPromptTokens;
    this.accumulatedCompletionTokens = accumulatedCompletionTokens;
    this.lastPromptTokens = lastPromptTokens;
    this.lastCompletionTokens = lastCompletionTokens;
  }

  public int totalSteps() {
    return totalSteps;
  }

  public long accumulatedPromptTokens() {
    return accumulatedPromptTokens;
  }

  public long accumulatedCompletionTokens() {
    return accumulatedCompletionTokens;
  }

  public long lastPromptTokens() {
    return lastPromptTokens;
  }

  public long lastCompletionTokens() {
    return lastCompletionTokens;
  }

  @Override
  public String toString() {
    return "AgentFinish{totalSteps="
        + totalSteps
        + ", accumulatedPromptTokens="
        + accumulatedPromptTokens
        + ", accumulatedCompletionTokens="
        + accumulatedCompletionTokens
        + ", lastPromptTokens="
        + lastPromptTokens
        + ", lastCompletionTokens="
        + lastCompletionTokens
        + "}";
  }
}
