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

import com.openai.client.OpenAIClient;
import com.openai.core.http.StreamResponse;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionChunk;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessageFunctionToolCall;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionStreamOptions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentDelta;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Streams one chat completion call and assembles assistant content plus streamed tool calls. */
final class LlmStreamClient {

  private static final Logger LOG = LoggerFactory.getLogger(LlmStreamClient.class);

  private final OpenAIClient client;
  private final ToolRegistry toolRegistry;

  LlmStreamClient(OpenAIClient client, ToolRegistry toolRegistry) {
    this.client = client;
    this.toolRegistry = toolRegistry;
  }

  /**
   * Stream LLM response, emitting ContentDelta through {@code ctx} for each text chunk. Assembles
   * tool calls directly from streamed chunks with no non-streaming fallback.
   */
  StreamResult stream(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages, String effectiveModel) {
    ChatCompletionCreateParams.Builder paramsBuilder =
        ChatCompletionCreateParams.builder()
            .model(effectiveModel)
            .streamOptions(ChatCompletionStreamOptions.builder().includeUsage(true).build());
    for (ChatCompletionMessageParam msg : messages) {
      paramsBuilder.addMessage(msg);
    }
    toolRegistry.addToolsTo(paramsBuilder);

    LOG.info("LLM request: model={}", effectiveModel);
    StreamAccumulator acc = new StreamAccumulator();
    try (StreamResponse<ChatCompletionChunk> stream =
        client.chat().completions().createStreaming(paramsBuilder.build())) {
      stream.stream().forEach(chunk -> consumeChunk(ctx, chunk, acc));
    }
    return new StreamResult(acc.content.toString(), acc.buildToolCalls());
  }

  /** Fold one streaming chunk into {@code acc}, emitting per-token {@link ContentDelta}s. */
  private void consumeChunk(AgentRunContext ctx, ChatCompletionChunk chunk, StreamAccumulator acc) {
    if (!acc.serverModelLogged) {
      LOG.info("LLM response: server-echoed model={}", chunk.model());
      acc.serverModelLogged = true;
    }
    chunk
        .usage()
        .ifPresent(u -> ctx.addTokenUsage(u.promptTokens(), u.completionTokens(), u.totalTokens()));

    for (ChatCompletionChunk.Choice c : chunk.choices()) {
      c.delta()
          .content()
          .ifPresent(
              text -> {
                acc.content.append(text);
                ctx.emit(new ContentDelta(text));
              });
      c.delta().toolCalls().ifPresent(acc::mergeToolCallDeltas);
    }
  }

  /**
   * Mutable accumulator for a single streaming LLM turn. Tool call fields are keyed by the chunk's
   * {@code index} because provider SDKs may deliver a single logical call across multiple chunks
   * and only surface the {@code id}/{@code name} on the first one.
   */
  private static final class StreamAccumulator {
    final StringBuilder content = new StringBuilder();
    final Map<Integer, String> toolCallIds = new HashMap<>();
    final Map<Integer, String> toolCallNames = new HashMap<>();
    final Map<Integer, StringBuilder> toolCallArgs = new HashMap<>();
    boolean serverModelLogged = false;

    void mergeToolCallDeltas(List<ChatCompletionChunk.Choice.Delta.ToolCall> deltas) {
      for (ChatCompletionChunk.Choice.Delta.ToolCall tc : deltas) {
        int idx = (int) tc.index();
        tc.id().ifPresent(id -> toolCallIds.put(idx, id));
        tc.function()
            .ifPresent(
                fn -> {
                  fn.name().ifPresent(name -> toolCallNames.put(idx, name));
                  fn.arguments()
                      .ifPresent(
                          args ->
                              toolCallArgs
                                  .computeIfAbsent(idx, k -> new StringBuilder())
                                  .append(args));
                });
      }
    }

    /**
     * Materialize accumulated deltas into SDK tool-call objects. Returns {@code null} (not an empty
     * list) if no tool calls were seen, matching the existing {@link StreamResult} contract.
     */
    List<ChatCompletionMessageToolCall> buildToolCalls() {
      if (toolCallIds.isEmpty()) return null;
      List<ChatCompletionMessageToolCall> out = new ArrayList<>(toolCallIds.size());
      for (Map.Entry<Integer, String> e : toolCallIds.entrySet()) {
        int idx = e.getKey();
        String id = (e.getValue() == null || e.getValue().isEmpty()) ? synthId() : e.getValue();
        String args = toolCallArgs.containsKey(idx) ? toolCallArgs.get(idx).toString() : "{}";
        out.add(
            ChatCompletionMessageToolCall.ofFunction(
                ChatCompletionMessageFunctionToolCall.builder()
                    .id(id)
                    .function(
                        ChatCompletionMessageFunctionToolCall.Function.builder()
                            .name(toolCallNames.getOrDefault(idx, ""))
                            .arguments(args)
                            .build())
                    .build()));
      }
      return out;
    }

    /**
     * Synthesize an id for tool calls whose id never arrived on the stream (some OpenAI-compatible
     * providers omit it). The id has to be stable within a turn and unique across turns so the
     * assistant/tool_result pairing downstream holds.
     */
    private static String synthId() {
      return "local_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 24);
    }
  }

  /** Result of a streaming LLM call, assembled from chunks. */
  static final class StreamResult {
    final String content;
    final List<ChatCompletionMessageToolCall> toolCalls;

    StreamResult(String content, List<ChatCompletionMessageToolCall> toolCalls) {
      this.content = content;
      this.toolCalls = toolCalls;
    }

    boolean isEmpty() {
      return content.isEmpty() && (toolCalls == null || toolCalls.isEmpty());
    }

    /** Build the SDK assistant message corresponding to this streamed result. */
    ChatCompletionAssistantMessageParam toAssistantMessage() {
      ChatCompletionAssistantMessageParam.Builder b = ChatCompletionAssistantMessageParam.builder();
      if (!content.isEmpty()) {
        b.content(content);
      }
      if (toolCalls != null && !toolCalls.isEmpty()) {
        b.toolCalls(toolCalls);
      }
      return b.build();
    }
  }
}
