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
 * A chunk of the LLM's hidden reasoning / chain-of-thought stream. Emitted alongside {@link
 * ContentDelta} when the provider exposes a {@code reasoning_content} delta field (e.g. qwen3,
 * DeepSeek-R1). UI clients typically render these in a collapsible "thinking" panel.
 */
public final class ReasoningDelta extends AgentEvent {
  private final String text;

  public ReasoningDelta(String text) {
    super(EventType.REASONING_DELTA);
    this.text = text;
  }

  public String text() {
    return text;
  }

  @Override
  public String toString() {
    String preview = text != null && text.length() > 200 ? text.substring(0, 200) + "..." : text;
    return "ReasoningDelta{text='" + preview + "'}";
  }
}
