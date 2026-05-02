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

package org.apache.kyuubi.engine.dataagent.provider.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.engine.dataagent.datasource.DataSourceFactory;
import org.apache.kyuubi.engine.dataagent.provider.DataAgentProvider;
import org.apache.kyuubi.engine.dataagent.provider.ProviderRunRequest;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentFinish;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentStart;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentComplete;
import org.apache.kyuubi.engine.dataagent.runtime.event.ContentDelta;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepEnd;
import org.apache.kyuubi.engine.dataagent.runtime.event.StepStart;
import org.apache.kyuubi.engine.dataagent.runtime.event.ToolCall;
import org.apache.kyuubi.engine.dataagent.runtime.event.ToolResult;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunSelectQueryTool;
import org.apache.kyuubi.engine.dataagent.util.ConfUtils;

/**
 * A mock LLM provider for testing the full tool-call pipeline without a real LLM. Simulates the
 * ReAct loop: extracts SQL from the user question, executes it via SqlQueryTool, and returns the
 * result as a formatted answer.
 *
 * <p>Recognizes two patterns:
 *
 * <ul>
 *   <li>Questions containing SQL keywords (SELECT, SHOW, DESCRIBE) — extracts and executes the SQL
 *   <li>All other questions — returns a canned response without tool calls
 * </ul>
 */
public class MockLlmProvider implements DataAgentProvider {

  private static final Pattern SQL_PATTERN =
      Pattern.compile(
          "(SELECT\\b.+|SHOW\\b.+|DESCRIBE\\b.+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * Simple natural-language-to-SQL mappings so tests can use human-readable questions instead of
   * raw SQL. Checked before the regex pattern — if a question matches a key (case-insensitive
   * prefix), the mapped SQL is executed.
   */
  private static final Map<String, String> NL_TO_SQL = new java.util.LinkedHashMap<>();

  static {
    NL_TO_SQL.put(
        "list all employee names and departments",
        "SELECT name, department FROM employees ORDER BY id");
    NL_TO_SQL.put(
        "how many employees in each department",
        "SELECT department, COUNT(*) as cnt FROM employees GROUP BY department");
    NL_TO_SQL.put("count the total number of employees", "SELECT COUNT(*) FROM employees");
  }

  private final ConcurrentHashMap<String, Object> sessions = new ConcurrentHashMap<>();
  private final ToolRegistry toolRegistry;
  private final DataSource dataSource;

  public MockLlmProvider(KyuubiConf conf) {
    String jdbcUrl = ConfUtils.requireString(conf, KyuubiConf.ENGINE_DATA_AGENT_JDBC_URL());
    this.dataSource = DataSourceFactory.create(jdbcUrl);
    this.toolRegistry = new ToolRegistry(30);
    this.toolRegistry.register(new RunSelectQueryTool(dataSource, 0));
  }

  @Override
  public void open(String sessionId, String user) {
    sessions.put(sessionId, new Object());
  }

  @Override
  public void run(String sessionId, ProviderRunRequest request, Consumer<AgentEvent> onEvent) {
    String question = request.getQuestion();
    onEvent.accept(new AgentStart());

    // Trigger an error for testing the error path in ExecuteStatement
    if (question.trim().equalsIgnoreCase("__error__")) {
      throw new RuntimeException("MockLlmProvider simulated failure");
    }

    // First check natural-language mappings, then fall back to SQL pattern extraction
    String sql = resolveToSql(question);
    if (sql != null) {
      runWithToolCall(sql, onEvent);
    } else {
      runWithoutToolCall(question, onEvent);
    }
  }

  private void runWithToolCall(String sql, Consumer<AgentEvent> onEvent) {
    // Step 1: LLM "decides" to call sql_query tool
    onEvent.accept(new StepStart(1));
    String toolCallId = "mock_call_" + System.nanoTime();
    Map<String, Object> toolArgs = new HashMap<>();
    toolArgs.put("sql", sql);
    onEvent.accept(new ToolCall(toolCallId, "run_select_query", toolArgs));

    // Execute the tool
    String toolOutput =
        toolRegistry.executeTool("run_select_query", "{\"sql\":\"" + escapeJson(sql) + "\"}");
    onEvent.accept(new ToolResult(toolCallId, "run_select_query", toolOutput, false));
    onEvent.accept(new StepEnd(1));

    // Step 2: LLM "summarizes" the result
    onEvent.accept(new StepStart(2));
    String answer = "Based on the query result:\n\n" + toolOutput;
    for (String token : answer.split("(?<=\\n)")) {
      onEvent.accept(new ContentDelta(token));
    }
    onEvent.accept(new ContentComplete(answer));
    onEvent.accept(new StepEnd(2));
    onEvent.accept(new AgentFinish(2, 100, 50, 150));
  }

  private void runWithoutToolCall(String question, Consumer<AgentEvent> onEvent) {
    onEvent.accept(new StepStart(1));
    String answer = "[MockLLM] No SQL detected in: " + question;
    onEvent.accept(new ContentDelta(answer));
    onEvent.accept(new ContentComplete(answer));
    onEvent.accept(new StepEnd(1));
    onEvent.accept(new AgentFinish(1, 50, 20, 70));
  }

  @Override
  public void close(String sessionId) {
    sessions.remove(sessionId);
  }

  @Override
  public void stop() {
    if (dataSource instanceof com.zaxxer.hikari.HikariDataSource) {
      ((com.zaxxer.hikari.HikariDataSource) dataSource).close();
    }
  }

  /**
   * Resolve a user question to SQL. Checks NL_TO_SQL mappings first, then falls back to regex
   * extraction of raw SQL from the input.
   */
  private static String resolveToSql(String question) {
    String lower = question.toLowerCase().trim();
    for (Map.Entry<String, String> entry : NL_TO_SQL.entrySet()) {
      if (lower.startsWith(entry.getKey())) {
        return entry.getValue();
      }
    }
    Matcher matcher = SQL_PATTERN.matcher(question);
    if (matcher.find()) {
      return matcher.group(1).trim();
    }
    return null;
  }

  private static String escapeJson(String s) {
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
