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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.core.JsonValue;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionFunctionTool;
import com.openai.models.chat.completions.ChatCompletionTool;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Registry for agent tools. Provides tool lookup, schema generation for LLM requests, and
 * deserialization + execution of tool calls.
 *
 * <p>Tool metadata (name, description) comes from {@link AgentTool}. Parameter schemas are
 * generated from the args class via {@link ToolSchemaGenerator}. The SDK's {@code
 * function.arguments(Class)} is used for deserializing LLM responses.
 */
public class ToolRegistry {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final Map<String, AgentTool<?>> tools = new LinkedHashMap<>();
  private volatile Map<String, ChatCompletionTool> cachedSpecs;

  /** Register a tool. Keyed by {@link AgentTool#name()}. */
  public synchronized ToolRegistry register(AgentTool<?> tool) {
    tools.put(tool.name(), tool);
    cachedSpecs = null; // invalidate cache
    return this;
  }

  public synchronized boolean isEmpty() {
    return tools.isEmpty();
  }

  /** Returns the risk level of the named tool, or {@link ToolRiskLevel#SAFE} if not found. */
  public synchronized ToolRiskLevel getRiskLevel(String toolName) {
    AgentTool<?> tool = tools.get(toolName);
    return tool != null ? tool.riskLevel() : ToolRiskLevel.SAFE;
  }

  /** Add all tools to the ChatCompletion request builder. */
  public void addToolsTo(ChatCompletionCreateParams.Builder builder) {
    Map<String, ChatCompletionTool> specs = ensureSpecs();
    for (ChatCompletionTool spec : specs.values()) {
      builder.addTool(spec);
    }
  }

  private synchronized Map<String, ChatCompletionTool> ensureSpecs() {
    if (cachedSpecs == null) {
      Map<String, ChatCompletionTool> specs = new LinkedHashMap<>();
      for (AgentTool<?> tool : tools.values()) {
        specs.put(tool.name(), buildChatCompletionTool(tool));
      }
      cachedSpecs = specs;
    }
    return cachedSpecs;
  }

  /**
   * Execute a tool call: deserialize the JSON args, then delegate to the tool.
   *
   * @param toolName the function name from the LLM response
   * @param argsJson the raw JSON arguments string
   * @return the result string, or an error message
   */
  @SuppressWarnings("unchecked")
  public String executeTool(String toolName, String argsJson) {
    AgentTool<?> tool;
    synchronized (this) {
      tool = tools.get(toolName);
    }
    if (tool == null) {
      return "Error: unknown tool '" + toolName + "'";
    }
    return executeTyped((AgentTool<Object>) tool, argsJson);
  }

  private static <T> String executeTyped(AgentTool<T> tool, String argsJson) {
    try {
      T args = JSON.readValue(argsJson, tool.argsType());
      return tool.execute(args);
    } catch (Exception e) {
      return "Error executing " + tool.name() + ": " + e.getMessage();
    }
  }

  private static ChatCompletionTool buildChatCompletionTool(AgentTool<?> tool) {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(tool.argsType());

    FunctionParameters.Builder paramsBuilder = FunctionParameters.builder();
    for (Map.Entry<String, Object> entry : schema.entrySet()) {
      paramsBuilder.putAdditionalProperty(entry.getKey(), JsonValue.from(entry.getValue()));
    }

    return ChatCompletionTool.ofFunction(
        ChatCompletionFunctionTool.builder()
            .function(
                FunctionDefinition.builder()
                    .name(tool.name())
                    .description(tool.description())
                    .parameters(paramsBuilder.build())
                    .build())
            .build());
  }
}
