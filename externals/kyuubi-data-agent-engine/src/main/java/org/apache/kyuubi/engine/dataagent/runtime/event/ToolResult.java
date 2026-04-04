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

/** The result of a tool invocation. */
public final class ToolResult extends AgentEvent {
  private final String toolCallId;
  private final String toolName;
  private final String output;
  private final boolean isError;

  public ToolResult(String toolCallId, String toolName, String output, boolean isError) {
    super(EventType.TOOL_RESULT);
    this.toolCallId = toolCallId;
    this.toolName = toolName;
    this.output = output;
    this.isError = isError;
  }

  public String toolCallId() {
    return toolCallId;
  }

  public String toolName() {
    return toolName;
  }

  public String output() {
    return output;
  }

  public boolean isError() {
    return isError;
  }

  @Override
  public String toString() {
    return "ToolResult{id='"
        + toolCallId
        + "', toolName='"
        + toolName
        + "', isError="
        + isError
        + ", output='"
        + (output != null && output.length() > 200 ? output.substring(0, 200) + "..." : output)
        + "'}";
  }
}
