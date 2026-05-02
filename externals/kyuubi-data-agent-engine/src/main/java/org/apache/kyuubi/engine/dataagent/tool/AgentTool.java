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

package org.apache.kyuubi.engine.dataagent.tool;

/**
 * Base interface for agent tools. Separates tool metadata (name, description) and execution logic
 * from the parameter schema (the args class with {@code @JsonPropertyDescription} annotations).
 *
 * <p>The args class defines parameter fields for JSON Schema generation and SDK deserialization.
 * The tool implementation holds runtime dependencies (e.g. DataSource) and performs the actual
 * work.
 *
 * @param <T> the args class with fields annotated by {@code @JsonPropertyDescription}
 */
public interface AgentTool<T> {

  /** Unique name for this tool, used by the LLM to select it. */
  String name();

  /** Description shown to the LLM to help it decide when to use this tool. */
  String description();

  /** Returns the args class for JSON Schema generation and deserialization. */
  Class<T> argsType();

  /**
   * Returns the risk level of this tool, used to determine whether user approval is required.
   * Defaults to {@link ToolRiskLevel#SAFE}.
   */
  default ToolRiskLevel riskLevel() {
    return ToolRiskLevel.SAFE;
  }

  /**
   * Execute the tool with the given deserialized arguments.
   *
   * @param ctx per-invocation context (session id, etc.); never null — use {@link
   *     ToolContext#EMPTY} for calls without a session. Tools that are session-agnostic may ignore
   *     it.
   * @param args the deserialized arguments from the LLM's tool call
   * @return the result string to feed back to the LLM
   */
  String execute(ToolContext ctx, T args);
}
