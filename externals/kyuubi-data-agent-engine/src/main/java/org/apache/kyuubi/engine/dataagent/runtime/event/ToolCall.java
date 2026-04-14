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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** The agent is about to invoke a tool. */
public final class ToolCall extends AgentEvent {
  private final String toolCallId;
  private final String toolName;
  private final Map<String, Object> toolArgs;

  public ToolCall(String toolCallId, String toolName, Map<String, Object> toolArgs) {
    super(EventType.TOOL_CALL);
    this.toolCallId = toolCallId;
    this.toolName = toolName;
    this.toolArgs =
        toolArgs != null
            ? Collections.unmodifiableMap(new LinkedHashMap<>(toolArgs))
            : Collections.emptyMap();
  }

  public String toolCallId() {
    return toolCallId;
  }

  public String toolName() {
    return toolName;
  }

  public Map<String, Object> toolArgs() {
    return toolArgs;
  }

  @Override
  public String toString() {
    return "ToolCall{id='"
        + toolCallId
        + "', toolName='"
        + toolName
        + "', toolArgs="
        + toolArgs
        + "}";
  }
}
