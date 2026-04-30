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

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.chat.completions.ChatCompletionAssistantMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageFunctionToolCall;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionToolMessageParam;
import com.openai.models.chat.completions.ChatCompletionUserMessageParam;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ApprovalMode;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests that exercise only the deterministic, LLM-free parts of {@link CompactionMiddleware}:
 *
 * <ul>
 *   <li>Static helpers {@code computeSplit} and {@code estimateTailAfterLastAssistant}.
 *   <li>Pre-summarizer gating paths in {@code beforeLlmCall} (below threshold, empty old after
 *       split) that never reach the LLM call.
 *   <li>Constructor validation.
 * </ul>
 *
 * The end-to-end behaviour (actual summarization output, compacted-history structure, no re-trigger
 * next turn) is covered by {@code CompactionMiddlewareLiveTest}, which is gated on real LLM
 * credentials.
 */
public class CompactionMiddlewareTest {

  /**
   * A minimally-configured real {@link OpenAIClient}. Never invoked in these tests because every
   * code path exercised here returns before reaching the summarizer. Uses dummy credentials and a
   * bogus base URL so an accidental invocation would fail loudly.
   */
  private static final OpenAIClient DUMMY_CLIENT =
      OpenAIOkHttpClient.builder()
          .apiKey("dummy-for-unit-tests")
          .baseUrl("http://127.0.0.1:0")
          .build();

  private ConversationMemory memory;
  private AgentRunContext ctx;

  @Before
  public void setUp() {
    memory = new ConversationMemory();
    memory.setSystemPrompt("SYS");
    ctx = new AgentRunContext(memory, ApprovalMode.AUTO_APPROVE);
  }

  // ----- computeSplit -----

  @Test
  public void computeSplit_shortHistoryReturnsAllInKept() {
    List<ChatCompletionMessageParam> history =
        Arrays.asList(userMsg("u0"), asstMsg(asstPlain("a0")));
    CompactionMiddleware.Split s = CompactionMiddleware.computeSplit(history, 4);
    assertEquals(0, s.old.size());
    assertEquals(2, s.kept.size());
  }

  @Test
  public void computeSplit_simpleAlternationSplitsCorrectly() {
    // 10 msgs: u0,a0,u1,a1,u2,a2,u3,a3,u4,a4.  keep=2 → boundary at u3 (2 users from tail).
    List<ChatCompletionMessageParam> history = alternatingHistory(10);
    CompactionMiddleware.Split s = CompactionMiddleware.computeSplit(history, 2);
    // 4 users from tail: u4,u3 → splitIdx = index of u3 = 6 → old=[0..5], kept=[6..9]
    assertEquals(6, s.old.size());
    assertEquals(4, s.kept.size());
    assertTrue(s.kept.get(0).isUser());
  }

  @Test
  public void computeSplit_neverOrphansToolResult() {
    // Layout: u0 a0 u1 a1 u2 a2 u3 a3(tc1) tool(tc1) u4 a4 u5 a5 u6 a6 u7 a7
    // keep=4 → naive splitIdx lands between tool(tc1) and u4; pair-protection must shift it back
    // to before a3(tc1), so a3(tc1) + tool(tc1) end up in kept.
    List<ChatCompletionMessageParam> history = new ArrayList<>();
    history.add(userMsg("u0"));
    history.add(asstMsg(asstPlain("a0")));
    history.add(userMsg("u1"));
    history.add(asstMsg(asstPlain("a1")));
    history.add(userMsg("u2"));
    history.add(asstMsg(asstPlain("a2")));
    history.add(userMsg("u3"));
    history.add(asstMsg(asstWithToolCall("a3", "tc1", "sql_query", "{}")));
    history.add(toolMsg("tc1", "r1"));
    history.add(userMsg("u4"));
    history.add(asstMsg(asstPlain("a4")));
    history.add(userMsg("u5"));
    history.add(asstMsg(asstPlain("a5")));
    history.add(userMsg("u6"));
    history.add(asstMsg(asstPlain("a6")));
    history.add(userMsg("u7"));
    history.add(asstMsg(asstPlain("a7")));

    CompactionMiddleware.Split s = CompactionMiddleware.computeSplit(history, 4);
    assertNoOrphanToolResult(s.kept);
    // Verify the tc1 pair really did end up in kept, not old.
    assertTrue("a3(tc1) must be in kept", containsToolCallId(s.kept, "tc1"));
    assertTrue("tool_result(tc1) must be in kept", containsToolCallIdAsResult(s.kept, "tc1"));
  }

  @Test
  public void computeSplit_keepCountExceedsAvailableUsers() {
    // Only 2 user msgs but we ask to keep 4 — boundary walks to the top, old=[], kept=all.
    List<ChatCompletionMessageParam> history = alternatingHistory(4); // u0,a0,u1,a1
    CompactionMiddleware.Split s = CompactionMiddleware.computeSplit(history, 4);
    assertEquals(0, s.old.size());
    assertEquals(4, s.kept.size());
  }

  // ----- estimateTailAfterLastAssistant -----

  @Test
  public void estimateTail_afterLastAssistant() {
    // u(200 chars) a(50) u(100) → last assistant at index 1, tail is the final user = 100 chars
    // → 100/4 = 25 tokens
    List<ChatCompletionMessageParam> msgs =
        Arrays.asList(
            userMsg(repeat('x', 200)),
            asstMsg(asstPlain(repeat('y', 50))),
            userMsg(repeat('z', 100)));
    assertEquals(25L, CompactionMiddleware.estimateTailAfterLastAssistant(msgs));
  }

  @Test
  public void estimateTail_noAssistantMeansEverythingIsTail() {
    List<ChatCompletionMessageParam> msgs =
        Arrays.asList(userMsg(repeat('x', 400)), userMsg(repeat('y', 400)));
    assertEquals(200L, CompactionMiddleware.estimateTailAfterLastAssistant(msgs));
  }

  @Test
  public void estimateTail_emptyReturnsZero() {
    assertEquals(0L, CompactionMiddleware.estimateTailAfterLastAssistant(Collections.emptyList()));
  }

  // ----- beforeLlmCall pre-summarizer gating -----

  @Test
  public void belowThresholdReturnsNull() {
    seedSimpleHistory(6);
    ctx.addTokenUsage(1000, 0, 1000);
    CompactionMiddleware mw = new CompactionMiddleware(DUMMY_CLIENT, "m", 50_000L);

    assertEquals(Decision.Kind.PROCEED, mw.beforeLlmCall(ctx, memory.buildLlmMessages()).kind());
    // Nothing was mutated.
    assertEquals(6, memory.size());
  }

  @Test
  public void aboveThresholdButHistoryTooShortReturnsNull() {
    // Threshold crossed (60k cumulative) but history has only 2 user turns → computeSplit
    // can't satisfy KEEP_RECENT_TURNS=4 and keeps everything, leaving split.old empty; so
    // beforeLlmCall bails out before ever invoking the summarizer.
    memory.addUserMessage("u0");
    memory.addAssistantMessage(asstPlain("a0"));
    memory.addUserMessage("u1");
    ctx.addTokenUsage(60_000, 0, 60_000);
    CompactionMiddleware mw = new CompactionMiddleware(DUMMY_CLIENT, "m", 50_000L);

    assertEquals(Decision.Kind.PROCEED, mw.beforeLlmCall(ctx, memory.buildLlmMessages()).kind());
    assertEquals(3, memory.size());
  }

  @Test
  public void triggerUsesLastCallTotalNotCumulative() {
    // Two consecutive calls with total_tokens below threshold. The middleware must key on the
    // *last* call's total (prompt + completion), not the session cumulative — otherwise a session
    // that has accumulated large cumulative cost but then compacted would misfire. Using total
    // (not just prompt) also covers the last assistant message — e.g. a tool_call's completion
    // tokens — which is part of the next prompt but sits beyond the tail estimator's window.
    seedSimpleHistory(6);
    CompactionMiddleware mw = new CompactionMiddleware(DUMMY_CLIENT, "m", 50_000L);

    ctx.addTokenUsage(4_000, 1_000, 5_000);
    assertEquals(Decision.Kind.PROCEED, mw.beforeLlmCall(ctx, memory.buildLlmMessages()).kind());

    ctx.addTokenUsage(8_000, 2_000, 10_000);
    assertEquals(Decision.Kind.PROCEED, mw.beforeLlmCall(ctx, memory.buildLlmMessages()).kind());

    assertEquals(10_000L, memory.getLastTotalTokens());
    assertEquals(15_000L, memory.getCumulativeTotalTokens());
  }

  // ----- helpers -----

  private void seedSimpleHistory(int n) {
    for (int i = 0; i < n; i++) {
      if (i % 2 == 0) {
        memory.addUserMessage("u" + i);
      } else {
        memory.addAssistantMessage(asstPlain("a" + i));
      }
    }
  }

  private static List<ChatCompletionMessageParam> alternatingHistory(int n) {
    List<ChatCompletionMessageParam> out = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      if (i % 2 == 0) {
        out.add(userMsg("u" + i));
      } else {
        out.add(asstMsg(asstPlain("a" + i)));
      }
    }
    return out;
  }

  private static ChatCompletionMessageParam userMsg(String text) {
    return ChatCompletionMessageParam.ofUser(
        ChatCompletionUserMessageParam.builder().content(text).build());
  }

  private static ChatCompletionMessageParam asstMsg(ChatCompletionAssistantMessageParam p) {
    return ChatCompletionMessageParam.ofAssistant(p);
  }

  private static ChatCompletionMessageParam toolMsg(String toolCallId, String content) {
    return ChatCompletionMessageParam.ofTool(
        ChatCompletionToolMessageParam.builder().toolCallId(toolCallId).content(content).build());
  }

  private static ChatCompletionAssistantMessageParam asstPlain(String text) {
    return ChatCompletionAssistantMessageParam.builder().content(text).build();
  }

  private static ChatCompletionAssistantMessageParam asstWithToolCall(
      String text, String toolCallId, String toolName, String args) {
    List<ChatCompletionMessageToolCall> calls = new ArrayList<>();
    calls.add(
        ChatCompletionMessageToolCall.ofFunction(
            ChatCompletionMessageFunctionToolCall.builder()
                .id(toolCallId)
                .function(
                    ChatCompletionMessageFunctionToolCall.Function.builder()
                        .name(toolName)
                        .arguments(args)
                        .build())
                .build()));
    return ChatCompletionAssistantMessageParam.builder().content(text).toolCalls(calls).build();
  }

  private static String repeat(char c, int n) {
    char[] arr = new char[n];
    Arrays.fill(arr, c);
    return new String(arr);
  }

  private static boolean containsToolCallId(List<ChatCompletionMessageParam> msgs, String id) {
    for (ChatCompletionMessageParam m : msgs) {
      if (m.isAssistant()) {
        List<ChatCompletionMessageToolCall> calls = m.asAssistant().toolCalls().orElse(null);
        if (calls == null) continue;
        for (ChatCompletionMessageToolCall tc : calls) {
          if (tc.isFunction() && id.equals(tc.asFunction().id())) return true;
        }
      }
    }
    return false;
  }

  private static boolean containsToolCallIdAsResult(
      List<ChatCompletionMessageParam> msgs, String id) {
    for (ChatCompletionMessageParam m : msgs) {
      if (m.isTool() && id.equals(m.asTool().toolCallId())) return true;
    }
    return false;
  }

  private static void assertNoOrphanToolResult(List<ChatCompletionMessageParam> msgs) {
    Set<String> issued = new HashSet<>();
    for (ChatCompletionMessageParam m : msgs) {
      if (m.isAssistant()) {
        m.asAssistant()
            .toolCalls()
            .ifPresent(
                calls -> {
                  for (ChatCompletionMessageToolCall tc : calls) {
                    if (tc.isFunction()) issued.add(tc.asFunction().id());
                  }
                });
      }
    }
    for (ChatCompletionMessageParam m : msgs) {
      if (m.isTool()) {
        assertTrue(
            "tool_result id=" + m.asTool().toolCallId() + " has no matching tool_call",
            issued.contains(m.asTool().toolCallId()));
      }
    }
  }
}
