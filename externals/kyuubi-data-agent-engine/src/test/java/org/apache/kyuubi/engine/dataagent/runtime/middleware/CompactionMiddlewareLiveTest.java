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

package org.apache.kyuubi.engine.dataagent.runtime.middleware;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import java.util.List;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ApprovalMode;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.junit.Before;
import org.junit.Test;

/**
 * Live integration test for {@link CompactionMiddleware}: exercises the full compaction path
 * against a real OpenAI-compatible LLM. Requires {@code DATA_AGENT_OPENAI_API_KEY} and {@code
 * DATA_AGENT_OPENAI_ENDPOINT} environment variables; skipped otherwise.
 */
public class CompactionMiddlewareLiveTest {

  private static final String API_KEY =
      System.getenv().getOrDefault("DATA_AGENT_OPENAI_API_KEY", "");
  private static final String BASE_URL =
      System.getenv().getOrDefault("DATA_AGENT_OPENAI_ENDPOINT", "");
  private static final String MODEL_NAME = System.getenv().getOrDefault("DATA_AGENT_MODEL", "");

  private OpenAIClient client;

  @Before
  public void setUp() {
    assumeTrue("DATA_AGENT_OPENAI_API_KEY not set, skipping live tests", !API_KEY.isEmpty());
    assumeTrue("DATA_AGENT_OPENAI_ENDPOINT not set, skipping live tests", !BASE_URL.isEmpty());
    client = OpenAIOkHttpClient.builder().apiKey(API_KEY).baseUrl(BASE_URL).build();
  }

  @Test
  public void compactsHistoryWhenThresholdCrossed() {
    // Seed a realistic ReAct-style history so the summarizer has something non-trivial to
    // summarize. ~20 alternating user/assistant turns.
    ConversationMemory memory = new ConversationMemory();
    memory.setSystemPrompt(
        "You are a data agent. You previously helped the user investigate the orders table.");
    for (int i = 0; i < 10; i++) {
      memory.addUserMessage(
          "Follow-up question " + i + ": what about the column customer_id in orders?");
      memory.addAssistantMessage(
          ChatCompletionAssistantMessageParam.builder()
              .content(
                  "Assistant turn "
                      + i
                      + ": the orders table has a customer_id BIGINT column referencing customers.id.")
              .build());
    }
    int originalSize = memory.size();

    AgentRunContext ctx = new AgentRunContext(memory, ApprovalMode.AUTO_APPROVE);
    // Simulate the previous LLM call having reported a large prompt_tokens so the next
    // beforeLlmCall trips the threshold.
    ctx.addTokenUsage(60_000, 0, 60_000);

    CompactionMiddleware mw = new CompactionMiddleware(client, MODEL_NAME, /* trigger */ 50_000L);

    AgentMiddleware.LlmCallAction action = mw.beforeLlmCall(ctx, memory.buildLlmMessages());

    assertNotNull("expected compaction to fire", action);
    assertTrue(action instanceof AgentMiddleware.LlmModifyMessages);

    // History got rewritten: [summary user msg] + kept tail.
    List<ChatCompletionMessageParam> hist = memory.getHistory();
    assertTrue(hist.size() < originalSize);
    assertTrue(hist.get(0).isUser());
    String first = hist.get(0).asUser().content().text().orElse("");
    assertTrue(
        "summary message should be wrapped in <conversation_summary>",
        first.contains("<conversation_summary>"));

    // The LLM was told to emit 8 markdown sections; sanity-check a couple show up.
    assertTrue(
        "summary should contain '## User Intent' section",
        first.contains("## User Intent") || first.contains("User Intent"));
  }
}
