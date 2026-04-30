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

import com.openai.client.OpenAIClient;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionSystemMessageParam;
import com.openai.models.chat.completions.ChatCompletionUserMessageParam;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.apache.kyuubi.engine.dataagent.runtime.event.Compaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Middleware that compacts conversation history when the prompt grows large.
 *
 * <p><b>Trigger formula:</b>
 *
 * <pre>
 *   predicted_this_turn_prompt_tokens
 *       = last_llm_call_total_tokens            // prompt + completion of the previous call
 *       + estimate_new_tail(messages)           // chars / 4, for content appended after the
 *                                               // last assistant message (tool results, new user)
 * </pre>
 *
 * History (already sent to the LLM) must use real token counts — we read them straight off {@link
 * ConversationMemory#getLastTotalTokens()}, which the provider updates after every call. We use
 * <i>total</i> (prompt + completion) rather than just prompt because the previous call's completion
 * — e.g. an assistant {@code tool_call} message — is already appended to history and will be
 * tokenized into the next prompt; the tail estimator starts strictly <i>after</i> the last
 * assistant message, so without completion we would miss it. Only content appended since that
 * assistant message is estimated, which catches spikes before the next API report arrives.
 *
 * <p><b>Post-compaction persistence:</b> the summary + kept tail replace {@link
 * ConversationMemory}'s history via {@link ConversationMemory#replaceHistory}. All subsequent turns
 * in this session read the compacted history — we do not re-summarize each turn. Because the next
 * LLM call uses the compacted messages, its reported {@code prompt_tokens} will be small, naturally
 * preventing retriggering.
 *
 * <p><b>Thread safety / shared instance:</b> Instances of this middleware are shared across all
 * sessions inside a provider (see {@code ChatCompletionProvider} javadoc). All per-session state
 * (cumulative and last-call totals) lives on {@link ConversationMemory}, so this middleware itself
 * is stateless across sessions and requires no per-session cleanup.
 *
 * <p><b>Tool-call pair invariant:</b> The split never separates an assistant message bearing {@code
 * tool_calls} from the {@code tool_result} messages that satisfy those calls — an orphan
 * tool_result is rejected by the OpenAI API with HTTP 400.
 *
 * <p><b>Failure handling:</b> summarizer failures propagate out of {@code beforeLlmCall} to the
 * agent's top-level catch, which surfaces an {@code AgentError} event. We don't silently skip
 * compaction — a broken summarizer is a real problem the operator needs to see.
 *
 * <p><b>Disabling:</b> to effectively turn compaction off, construct with a very large {@code
 * triggerPromptTokens} (e.g., {@link Long#MAX_VALUE}).
 *
 * <p>TODO: support a separate (cheaper) summarization model distinct from the main agent model.
 */
public class CompactionMiddleware implements AgentMiddleware {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionMiddleware.class);

  /** Number of recent user turns (and their assistant/tool companions) to preserve verbatim. */
  private static final int KEEP_RECENT_TURNS = 4;

  private static final String COMPACTION_SYSTEM_PROMPT =
      "SYSTEM OPERATION — this is an automated context compaction step, NOT a user message.\n"
          + "\n"
          + "You are summarizing a conversation between a user and a Data Agent (a ReAct agent"
          + " that executes SQL and analytics tools against Kyuubi). The user has NOT asked you"
          + " to summarize anything. Do not address the user. Do not ask questions. Produce only"
          + " the summary in the schema below.\n"
          + "\n"
          + "Goal: produce a dense, structured summary the agent can resume from without losing"
          + " critical context. Preserve concrete details verbatim — file paths, table names,"
          + " schema definitions, SQL snippets, column names, error messages.\n"
          + "\n"
          + "Output EXACTLY these 8 sections, in order, as markdown headers:\n"
          + "\n"
          + "1. ## User Intent\n"
          + "   The user's original request, restated in full. Preserve the literal phrasing of"
          + " the ask. Include follow-up refinements.\n"
          + "\n"
          + "2. ## Key Concepts\n"
          + "   Domain terms, data sources, tables, schemas, SQL dialects, and business logic"
          + " the agent has been reasoning about.\n"
          + "\n"
          + "3. ## Files and Code\n"
          + "   File paths, query text, DDL, or code artifacts referenced. Include verbatim SQL"
          + " snippets that produced meaningful results.\n"
          + "\n"
          + "4. ## Errors and Recoveries\n"
          + "   Errors encountered (SQL syntax, permission, timeout, tool failures), what was"
          + " tried, and what resolved them. Preserve error messages verbatim.\n"
          + "\n"
          + "5. ## Pending Work\n"
          + "   Tasks the agent identified but has not completed yet.\n"
          + "\n"
          + "6. ## Current State\n"
          + "   Where the agent is right now — what question is open, what data has been"
          + " retrieved, what hypothesis is being tested.\n"
          + "\n"
          + "7. ## Next Step\n"
          + "   The immediate next action the agent should take when resuming.\n"
          + "\n"
          + "8. ## Tool Usage Summary\n"
          + "   Which tools were called, how many times, and notable results.\n"
          + "\n"
          + "CRITICAL:\n"
          + "- DO NOT ask the user about this summary.\n"
          + "- DO NOT mention that compaction occurred in any future assistant response.\n"
          + "- DO NOT invent details not present in the conversation.\n"
          + "- DO NOT output anything outside the 8 sections.\n";

  private final OpenAIClient client;
  private final String summarizerModel;
  private final long triggerPromptTokens;

  public CompactionMiddleware(
      OpenAIClient client, String summarizerModel, long triggerPromptTokens) {
    this.client = client;
    this.summarizerModel = summarizerModel;
    this.triggerPromptTokens = triggerPromptTokens;
  }

  @Override
  public LlmCallAction beforeLlmCall(
      AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
    ConversationMemory mem = ctx.getMemory();
    // 1) Real token count of the previous LLM call (prompt + completion, i.e. everything through
    //    the last assistant message, which is now part of history). 0 on the first call.
    long lastTotal = mem.getLastTotalTokens();
    // 2) Estimated tokens appended to the tail after the last assistant (tool_results, new user).
    long newTailEstimate = estimateTailAfterLastAssistant(messages);

    if (lastTotal + newTailEstimate < triggerPromptTokens) {
      return null;
    }

    List<ChatCompletionMessageParam> history = mem.getHistory();

    // 3) Split history into old (to summarize) and kept (recent tail), never orphaning a
    //    tool_result.
    Split split = computeSplit(history, KEEP_RECENT_TURNS);
    if (split.old.isEmpty()) {
      return null;
    }

    String summary = summarize(mem.getSystemPrompt(), split.old);

    // 4) Build the compacted history and persist into ConversationMemory.
    List<ChatCompletionMessageParam> compacted = new ArrayList<>(1 + split.kept.size());
    compacted.add(wrapSummaryAsUserMessage(summary));
    compacted.addAll(split.kept);
    mem.replaceHistory(compacted);

    LOG.info(
        "Compacted {} old msgs into 1 summary; kept {} tail msgs (lastTotal={}, newTail~={})",
        split.old.size(),
        split.kept.size(),
        lastTotal,
        newTailEstimate);

    ctx.emit(
        new Compaction(
            split.old.size(), split.kept.size(), triggerPromptTokens, lastTotal + newTailEstimate));

    return new LlmModifyMessages(mem.buildLlmMessages());
  }

  /** Call the LLM to produce a summary of {@code oldMessages}. Failures propagate. */
  private String summarize(String agentSystemPrompt, List<ChatCompletionMessageParam> oldMessages) {
    String systemPrompt = COMPACTION_SYSTEM_PROMPT;
    if (agentSystemPrompt != null && !agentSystemPrompt.isEmpty()) {
      systemPrompt =
          systemPrompt
              + "\n---\nFor context, the agent's own system prompt is:\n"
              + agentSystemPrompt;
    }

    String rendered = renderAsText(oldMessages);

    ChatCompletionCreateParams params =
        ChatCompletionCreateParams.builder()
            .model(summarizerModel)
            .temperature(0.0)
            .addMessage(
                ChatCompletionMessageParam.ofSystem(
                    ChatCompletionSystemMessageParam.builder().content(systemPrompt).build()))
            .addMessage(
                ChatCompletionMessageParam.ofUser(
                    ChatCompletionUserMessageParam.builder().content(rendered).build()))
            .build();

    ChatCompletion response = client.chat().completions().create(params);
    return response.choices().get(0).message().content().get();
  }

  // ----- helpers -----

  /** Sum of content characters in messages after the last assistant, using ~4 chars per token. */
  static long estimateTailAfterLastAssistant(List<ChatCompletionMessageParam> messages) {
    int lastAssistantIdx = -1;
    for (int i = messages.size() - 1; i >= 0; i--) {
      if (messages.get(i).isAssistant()) {
        lastAssistantIdx = i;
        break;
      }
    }
    long totalChars = 0;
    for (int i = lastAssistantIdx + 1; i < messages.size(); i++) {
      totalChars += contentCharCount(messages.get(i));
    }
    return totalChars / 4;
  }

  private static long contentCharCount(ChatCompletionMessageParam msg) {
    if (msg.isUser()) {
      return msg.asUser().content().text().map(String::length).orElse(0);
    }
    if (msg.isTool()) {
      return msg.asTool().content().text().map(String::length).orElse(0);
    }
    if (msg.isAssistant()) {
      return msg.asAssistant().content().flatMap(c -> c.text()).map(String::length).orElse(0);
    }
    if (msg.isSystem()) {
      return msg.asSystem().content().text().map(String::length).orElse(0);
    }
    return 0;
  }

  /**
   * Render a list of messages as plain text for the summarizer's user turn. Tool calls and tool
   * results are rendered as tagged text so the summarizer LLM doesn't try to continue them as live
   * agent state.
   */
  static String renderAsText(List<ChatCompletionMessageParam> messages) {
    StringBuilder sb = new StringBuilder();
    for (ChatCompletionMessageParam msg : messages) {
      if (sb.length() > 0) sb.append("\n\n");
      if (msg.isUser()) {
        sb.append("USER: ").append(extractUserContent(msg));
      } else if (msg.isAssistant()) {
        sb.append("ASSISTANT: ").append(extractAssistantContent(msg));
        msg.asAssistant()
            .toolCalls()
            .ifPresent(
                calls -> {
                  for (ChatCompletionMessageToolCall tc : calls) {
                    if (tc.isFunction()) {
                      sb.append("\n[tool_call: ")
                          .append(tc.asFunction().function().name())
                          .append("(")
                          .append(tc.asFunction().function().arguments())
                          .append(") id=")
                          .append(tc.asFunction().id())
                          .append("]");
                    }
                  }
                });
      } else if (msg.isTool()) {
        sb.append("[tool_result id=")
            .append(msg.asTool().toolCallId())
            .append("]: ")
            .append(extractToolContent(msg));
      } else if (msg.isSystem()) {
        // system prompt should not appear in oldMessages, but render defensively
        sb.append("SYSTEM: ").append(extractSystemContent(msg));
      }
    }
    return sb.toString();
  }

  private static String extractUserContent(ChatCompletionMessageParam msg) {
    return msg.asUser().content().text().orElse("[non-text content]");
  }

  private static String extractAssistantContent(ChatCompletionMessageParam msg) {
    return msg.asAssistant().content().map(c -> c.text().orElse("[non-text content]")).orElse("");
  }

  private static String extractToolContent(ChatCompletionMessageParam msg) {
    return msg.asTool().content().text().orElse("[non-text content]");
  }

  private static String extractSystemContent(ChatCompletionMessageParam msg) {
    return msg.asSystem().content().text().orElse("[non-text content]");
  }

  /** Result of splitting the history into a summarizable prefix and a kept tail. */
  static final class Split {

    final List<ChatCompletionMessageParam> old;
    final List<ChatCompletionMessageParam> kept;

    Split(List<ChatCompletionMessageParam> old, List<ChatCompletionMessageParam> kept) {
      this.old = old;
      this.kept = kept;
    }
  }

  /**
   * Split the history at a boundary that preserves the last {@code keepRecentTurns} user messages,
   * with adjustments so that no assistant-tool_use is separated from its tool_results.
   */
  static Split computeSplit(List<ChatCompletionMessageParam> history, int keepRecentTurns) {
    if (history.size() <= 2) {
      return new Split(new ArrayList<>(), new ArrayList<>(history));
    }

    // Walk from the tail, count user boundaries. If the history does not contain enough user
    // messages to satisfy keepRecentTurns, keep everything (splitIdx = 0); the empty-old check
    // in beforeLlmCall will then skip this turn gracefully.
    int userBoundariesFound = 0;
    int splitIdx = 0;
    for (int i = history.size() - 1; i >= 0; i--) {
      if (history.get(i).isUser()) {
        userBoundariesFound++;
        if (userBoundariesFound == keepRecentTurns) {
          splitIdx = i;
          break;
        }
      }
    }

    // Protect tool-call / tool-result pairing: never split between an assistant that issued
    // tool_calls and the tool_results that satisfy them.
    while (splitIdx > 0) {
      ChatCompletionMessageParam prev = history.get(splitIdx - 1);
      if (prev.isTool()) {
        splitIdx--;
        continue;
      }
      if (prev.isAssistant()) {
        boolean hasToolCalls = prev.asAssistant().toolCalls().map(List::size).orElse(0) > 0;
        if (hasToolCalls) {
          splitIdx--;
          continue;
        }
      }
      break;
    }

    // Also guard against the edge case: if kept contains a tool_result whose tool_call id is
    // defined only in old, pull that assistant (and its siblings) into kept too.
    Set<String> keptCallIds = collectToolCallIds(history.subList(splitIdx, history.size()));
    if (!keptCallIds.isEmpty()) {
      while (splitIdx > 0) {
        ChatCompletionMessageParam prev = history.get(splitIdx - 1);
        if (!prev.isAssistant()) break;
        List<ChatCompletionMessageToolCall> calls = prev.asAssistant().toolCalls().orElse(null);
        if (calls == null || calls.isEmpty()) break;
        boolean satisfiesKept = false;
        for (ChatCompletionMessageToolCall tc : calls) {
          if (tc.isFunction() && keptCallIds.contains(tc.asFunction().id())) {
            satisfiesKept = true;
            break;
          }
        }
        if (!satisfiesKept) break;
        splitIdx--;
      }
    }

    List<ChatCompletionMessageParam> oldPart = new ArrayList<>(history.subList(0, splitIdx));
    List<ChatCompletionMessageParam> keptPart =
        new ArrayList<>(history.subList(splitIdx, history.size()));
    return new Split(oldPart, keptPart);
  }

  private static Set<String> collectToolCallIds(List<ChatCompletionMessageParam> slice) {
    Set<String> ids = new HashSet<>();
    for (ChatCompletionMessageParam m : slice) {
      if (m.isTool()) {
        ids.add(m.asTool().toolCallId());
      }
    }
    return ids;
  }

  private static ChatCompletionMessageParam wrapSummaryAsUserMessage(String summary) {
    String body = "<conversation_summary>\n" + summary + "\n</conversation_summary>";
    return ChatCompletionMessageParam.ofUser(
        ChatCompletionUserMessageParam.builder().content(body).build());
  }
}
