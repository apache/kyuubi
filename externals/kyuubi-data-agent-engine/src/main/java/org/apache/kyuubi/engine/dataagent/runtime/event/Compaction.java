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
 * Emitted by {@code CompactionMiddleware} after it has summarized a prefix of the conversation
 * history and replaced it in memory. Purely observational — the LLM call that immediately follows
 * uses the already-compacted history, so consumers just see this as a side-channel notice that
 * compaction happened. The summary text itself is intentionally not included: it can be large and
 * would bloat the event stream; operators who need it can read the middleware log.
 */
public final class Compaction extends AgentEvent {
  private final int summarizedCount;
  private final int keptCount;
  private final long triggerTokens;
  private final long observedTokens;

  public Compaction(int summarizedCount, int keptCount, long triggerTokens, long observedTokens) {
    super(EventType.COMPACTION);
    this.summarizedCount = summarizedCount;
    this.keptCount = keptCount;
    this.triggerTokens = triggerTokens;
    this.observedTokens = observedTokens;
  }

  public int summarizedCount() {
    return summarizedCount;
  }

  public int keptCount() {
    return keptCount;
  }

  public long triggerTokens() {
    return triggerTokens;
  }

  public long observedTokens() {
    return observedTokens;
  }

  @Override
  public String toString() {
    return "Compaction{summarized="
        + summarizedCount
        + ", kept="
        + keptCount
        + ", triggerTokens="
        + triggerTokens
        + ", observedTokens="
        + observedTokens
        + "}";
  }
}
