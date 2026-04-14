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

/** The agent has finished its analysis. */
public final class AgentFinish extends AgentEvent {
  private final int totalSteps;
  private final long promptTokens;
  private final long completionTokens;
  private final long totalTokens;

  public AgentFinish(int totalSteps, long promptTokens, long completionTokens, long totalTokens) {
    super(EventType.AGENT_FINISH);
    this.totalSteps = totalSteps;
    this.promptTokens = promptTokens;
    this.completionTokens = completionTokens;
    this.totalTokens = totalTokens;
  }

  public int totalSteps() {
    return totalSteps;
  }

  public long promptTokens() {
    return promptTokens;
  }

  public long completionTokens() {
    return completionTokens;
  }

  public long totalTokens() {
    return totalTokens;
  }

  @Override
  public String toString() {
    return "AgentFinish{totalSteps="
        + totalSteps
        + ", promptTokens="
        + promptTokens
        + ", completionTokens="
        + completionTokens
        + ", totalTokens="
        + totalTokens
        + "}";
  }
}
