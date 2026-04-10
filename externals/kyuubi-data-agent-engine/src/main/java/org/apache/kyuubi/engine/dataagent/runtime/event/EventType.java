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
 * Enumerates the types of events emitted by the ReAct agent loop. Each value maps to a
 * corresponding {@link AgentEvent} subclass and carries an SSE event name used for wire
 * serialization.
 */
public enum EventType {

  /** The agent has started processing a user query. */
  AGENT_START("agent_start"),

  /** A new ReAct iteration is starting. */
  STEP_START("step_start"),

  /** A single token or chunk from the LLM streaming response. */
  CONTENT_DELTA("content_delta"),

  /** The complete LLM output for one reasoning step. */
  CONTENT_COMPLETE("content_complete"),

  /** The agent is about to invoke a tool. */
  TOOL_CALL("tool_call"),

  /** The result of a tool invocation. */
  TOOL_RESULT("tool_result"),

  /** A ReAct iteration has completed. */
  STEP_END("step_end"),

  /** An error occurred during agent execution. */
  ERROR("error"),

  /** The agent requires user approval before executing a tool. */
  APPROVAL_REQUEST("approval_request"),

  /** The agent has finished its analysis. */
  AGENT_FINISH("agent_finish");

  private final String sseEventName;

  EventType(String sseEventName) {
    this.sseEventName = sseEventName;
  }

  /** Returns the SSE event name used in the {@code event:} field of the SSE protocol. */
  public String sseEventName() {
    return sseEventName;
  }
}
