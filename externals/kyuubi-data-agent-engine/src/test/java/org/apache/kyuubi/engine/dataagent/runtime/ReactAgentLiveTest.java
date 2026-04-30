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

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kyuubi.engine.dataagent.prompt.SystemPromptBuilder;
import org.apache.kyuubi.engine.dataagent.runtime.event.*;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ApprovalMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.LoggingMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.ToolResultOffloadMiddleware;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunMutationQueryTool;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunSelectQueryTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sqlite.SQLiteDataSource;

/**
 * Live integration test with a real LLM and real SQLite database. Exercises the full ReAct loop:
 * LLM reasoning -> tool calls -> result verification. Requires DATA_AGENT_OPENAI_API_KEY and
 * DATA_AGENT_OPENAI_ENDPOINT environment variables. Works with any OpenAI-compatible LLM service.
 */
public class ReactAgentLiveTest {

  private static final String API_KEY =
      System.getenv().getOrDefault("DATA_AGENT_OPENAI_API_KEY", "");
  private static final String BASE_URL =
      System.getenv().getOrDefault("DATA_AGENT_OPENAI_ENDPOINT", "");
  private static final String MODEL_NAME =
      System.getenv().getOrDefault("DATA_AGENT_MODEL", "gpt-4o");

  private static final String SYSTEM_PROMPT =
      SystemPromptBuilder.create().datasource("sqlite").build();

  private final List<File> tempFiles = new ArrayList<>();
  private OpenAIClient client;

  @Before
  public void setUp() {
    assumeTrue("DATA_AGENT_OPENAI_API_KEY not set, skipping live tests", !API_KEY.isEmpty());
    assumeTrue("DATA_AGENT_OPENAI_ENDPOINT not set, skipping live tests", !BASE_URL.isEmpty());
    client = OpenAIOkHttpClient.builder().apiKey(API_KEY).baseUrl(BASE_URL).build();
  }

  @After
  public void tearDown() {
    tempFiles.forEach(File::delete);
  }

  @Test
  public void testPlainTextStreamingWithoutTools() {
    ReactAgent agent =
        ReactAgent.builder()
            .client(client)
            .modelName(MODEL_NAME)
            .toolRegistry(new ToolRegistry(30))
            .addMiddleware(new LoggingMiddleware())
            .maxIterations(3)
            .systemPrompt("You are a helpful assistant. Answer concisely in 1-2 sentences.")
            .build();

    List<AgentEvent> events = new CopyOnWriteArrayList<>();
    ConversationMemory memory = new ConversationMemory();

    agent.run(new AgentInvocation("What is Apache Kyuubi?"), memory, events::add);

    List<String> deltas =
        events.stream()
            .filter(e -> e instanceof ContentDelta)
            .map(e -> ((ContentDelta) e).text())
            .collect(Collectors.toList());

    assertTrue("Expected multiple ContentDelta events", deltas.size() > 1);
    assertFalse("Streamed text should not be empty", String.join("", deltas).isEmpty());
    assertTrue(events.stream().anyMatch(e -> e instanceof StepStart));
    assertTrue(events.stream().anyMatch(e -> e instanceof ContentComplete));
    assertTrue(events.get(events.size() - 1) instanceof AgentFinish);
    assertEquals(2, memory.getHistory().size()); // user + assistant
  }

  @Test
  public void testFullReActLoopWithSchemaInspectAndSqlQuery() {
    SQLiteDataSource ds = createSalesDatabase();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 0));

    ReactAgent agent =
        ReactAgent.builder()
            .client(client)
            .modelName(MODEL_NAME)
            .toolRegistry(registry)
            .addMiddleware(new LoggingMiddleware())
            .maxIterations(10)
            .systemPrompt(SYSTEM_PROMPT)
            .build();

    List<AgentEvent> events = new CopyOnWriteArrayList<>();
    ConversationMemory memory = new ConversationMemory();

    agent.run(
        new AgentInvocation(
            "What is the total revenue by product category? Which category has the highest revenue?"),
        memory,
        events::add);

    // Verify tool calls happened
    List<ToolCall> toolCalls =
        events.stream()
            .filter(e -> e instanceof ToolCall)
            .map(e -> (ToolCall) e)
            .collect(Collectors.toList());
    List<ToolResult> toolResults =
        events.stream()
            .filter(e -> e instanceof ToolResult)
            .map(e -> (ToolResult) e)
            .collect(Collectors.toList());

    assertFalse("Agent should have called at least one tool", toolCalls.isEmpty());
    assertFalse("Agent should have received tool results", toolResults.isEmpty());
    assertTrue("Tool calls should not error", toolResults.stream().noneMatch(ToolResult::isError));

    // Verify SQL query was called
    assertTrue(
        "Agent should execute at least one SQL query",
        toolCalls.stream().anyMatch(tc -> "run_select_query".equals(tc.toolName())));

    // Verify final answer mentions "Electronics" (highest revenue)
    List<String> completions =
        events.stream()
            .filter(e -> e instanceof ContentComplete)
            .map(e -> ((ContentComplete) e).fullText())
            .collect(Collectors.toList());
    String lastAnswer = completions.get(completions.size() - 1);
    assertTrue(
        "Final answer should mention Electronics, got: " + lastAnswer,
        lastAnswer.toLowerCase().contains("electronics"));

    // Verify agent finished successfully
    AgentEvent last = events.get(events.size() - 1);
    assertTrue(last instanceof AgentFinish);
    assertTrue("Should take multiple steps", ((AgentFinish) last).totalSteps() > 1);
  }

  @Test
  public void testMultiTurnConversationWithToolUse() {
    SQLiteDataSource ds = createSalesDatabase();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 0));

    ReactAgent agent =
        ReactAgent.builder()
            .client(client)
            .modelName(MODEL_NAME)
            .toolRegistry(registry)
            .addMiddleware(new LoggingMiddleware())
            .maxIterations(10)
            .systemPrompt(SYSTEM_PROMPT)
            .build();

    // Shared memory across turns
    ConversationMemory memory = new ConversationMemory();

    // Turn 1
    List<AgentEvent> events1 = new CopyOnWriteArrayList<>();
    agent.run(new AgentInvocation("How many orders are there in total?"), memory, events1::add);

    assertTrue(
        "Turn 1 should query the database",
        events1.stream()
            .anyMatch(
                e ->
                    e instanceof ToolCall && "run_select_query".equals(((ToolCall) e).toolName())));
    assertTrue(events1.get(events1.size() - 1) instanceof AgentFinish);

    // Turn 2: follow-up relying on conversation context
    List<AgentEvent> events2 = new CopyOnWriteArrayList<>();
    agent.run(
        new AgentInvocation("Now show me only orders above 500 dollars."), memory, events2::add);

    assertTrue(
        "Turn 2 should also query the database",
        events2.stream()
            .anyMatch(
                e ->
                    e instanceof ToolCall && "run_select_query".equals(((ToolCall) e).toolName())));
    assertTrue(events2.get(events2.size() - 1) instanceof AgentFinish);

    // Verify memory accumulated across both turns
    assertTrue(
        "Memory should contain messages from both turns, got " + memory.getHistory().size(),
        memory.getHistory().size() > 4);
  }

  @Test
  public void testToolOutputOffloadThenGrep() throws Exception {
    // Large result forces ToolResultOffloadMiddleware to truncate the tool output and
    // emit a preview hint telling the LLM to use grep_tool_output / read_tool_output.
    // A correct answer proves the LLM read the hint and drove the retrieval itself.
    SQLiteDataSource ds = createNeedleInHaystackDatabase();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 0));

    ReactAgent agent =
        ReactAgent.builder()
            .client(client)
            .modelName(MODEL_NAME)
            .toolRegistry(registry)
            .addMiddleware(new LoggingMiddleware())
            .addMiddleware(new ToolResultOffloadMiddleware())
            .maxIterations(10)
            .systemPrompt(SYSTEM_PROMPT)
            .build();

    List<AgentEvent> events = new CopyOnWriteArrayList<>();
    ConversationMemory memory = new ConversationMemory();

    // Explicit workflow: full-table scan first, then retrieval tool. Without this hint a
    // capable LLM just issues SELECT note FROM events WHERE tag='NEEDLE' and skips the
    // offload path entirely -- smart behavior, but defeats the purpose of this test.
    agent.run(
        new AgentInvocation(
                "Step 1: issue exactly this query: SELECT id, tag, note FROM events (no"
                    + " WHERE clause, return every row). Step 2: the result will be"
                    + " truncated; call the grep_tool_output tool with pattern 'NEEDLE' on"
                    + " the saved output file to find the matching row. Step 3: respond with"
                    + " ONLY the note text from that row, nothing else. Do NOT add a WHERE"
                    + " clause. Do NOT issue any other SQL.")
            // Offload middleware requires a non-null session id -- without it the offload
            // path skips entirely (see ToolResultOffloadMiddleware.afterToolCall).
            .sessionId("offload-live-" + java.util.UUID.randomUUID()),
        memory,
        events::add);

    List<ToolCall> toolCalls =
        events.stream()
            .filter(e -> e instanceof ToolCall)
            .map(e -> (ToolCall) e)
            .collect(Collectors.toList());
    List<ToolResult> toolResults =
        events.stream()
            .filter(e -> e instanceof ToolResult)
            .map(e -> (ToolResult) e)
            .collect(Collectors.toList());

    // Dump the tool trace -- diagnoses whether the LLM followed the workflow, added a
    // WHERE clause despite instructions, or deviated in some other way.
    for (ToolCall tc : toolCalls) {
      System.out.println("[ToolCall] " + tc.toolName() + " args=" + tc.toolArgs());
    }
    for (ToolResult tr : toolResults) {
      String out = tr.output();
      String preview =
          out.length() > 300
              ? out.substring(0, 300) + "...(+" + (out.length() - 300) + " chars)"
              : out;
      System.out.println("[ToolResult] " + tr.toolName() + " -> " + preview);
    }

    assertTrue(
        "Agent should have run a select query first",
        toolCalls.stream().anyMatch(tc -> "run_select_query".equals(tc.toolName())));

    // The SELECT returned 800 rows, which must trip the offload threshold.
    assertTrue(
        "Expected at least one offload preview marker in tool results",
        toolResults.stream().anyMatch(tr -> tr.output().contains("Tool output truncated")));

    assertTrue(
        "Agent should have used grep_tool_output or read_tool_output after seeing"
            + " the offload preview; actual tool calls: "
            + toolCalls.stream().map(ToolCall::toolName).collect(Collectors.toList()),
        toolCalls.stream()
            .anyMatch(
                tc ->
                    "grep_tool_output".equals(tc.toolName())
                        || "read_tool_output".equals(tc.toolName())));

    String finalAnswer =
        events.stream()
            .filter(e -> e instanceof ContentComplete)
            .map(e -> ((ContentComplete) e).fullText())
            .reduce((a, b) -> b)
            .orElse("");
    assertTrue(
        "Final answer should contain the needle note 'the-answer-is-42', got: " + finalAnswer,
        finalAnswer.contains("the-answer-is-42"));
  }

  @Test
  public void testApprovalApproveFlow() throws Exception {
    // Real LLM picks run_mutation_query (DESTRUCTIVE) -> ApprovalMiddleware pauses ->
    // background thread resolves(approved=true) -> mutation actually runs in SQLite.
    SQLiteDataSource ds = createCountersDatabase();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 0));
    registry.register(new RunMutationQueryTool(ds, 0));

    ApprovalMiddleware approval = new ApprovalMiddleware(30);

    ReactAgent agent =
        ReactAgent.builder()
            .client(client)
            .modelName(MODEL_NAME)
            .toolRegistry(registry)
            .addMiddleware(new LoggingMiddleware())
            .addMiddleware(approval)
            .maxIterations(10)
            .systemPrompt(SYSTEM_PROMPT)
            .build();

    List<AgentEvent> events = new CopyOnWriteArrayList<>();
    ConversationMemory memory = new ConversationMemory();

    // Auto-approve any approval request that shows up.
    ExecutorService approver = Executors.newSingleThreadExecutor();
    Consumer<AgentEvent> listener =
        event -> {
          events.add(event);
          if (event instanceof ApprovalRequest) {
            String rid = ((ApprovalRequest) event).requestId();
            approver.submit(() -> approval.resolve(rid, true));
          }
        };

    try {
      agent.run(
          new AgentInvocation(
              "Increment the 'hits' counter in the counters table by 1. After the update"
                  + " succeeds, run a SELECT query to read the new value back from the"
                  + " database, then respond with ONLY that value, no explanation."),
          memory,
          listener);
    } finally {
      approver.shutdown();
      approver.awaitTermination(5, TimeUnit.SECONDS);
    }

    List<ApprovalRequest> approvals =
        events.stream()
            .filter(e -> e instanceof ApprovalRequest)
            .map(e -> (ApprovalRequest) e)
            .collect(Collectors.toList());
    assertFalse("Expected at least one ApprovalRequest", approvals.isEmpty());
    assertTrue(
        "ApprovalRequest should target run_mutation_query",
        approvals.stream().anyMatch(a -> "run_mutation_query".equals(a.toolName())));

    // Mutation must have actually executed — check the DB directly.
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery("SELECT value FROM counters WHERE name='hits'")) {
      assertTrue(rs.next());
      assertEquals("Counter should be 1 after approved mutation", 1, rs.getInt(1));
    }

    String finalAnswer =
        events.stream()
            .filter(e -> e instanceof ContentComplete)
            .map(e -> ((ContentComplete) e).fullText())
            .reduce((a, b) -> b)
            .orElse("");
    assertTrue("Final answer should mention 1, got: " + finalAnswer, finalAnswer.contains("1"));
  }

  @Test
  public void testApprovalDenyFlow() throws Exception {
    // Same setup as the approve test, but the approval listener denies. The mutation
    // must NOT run, and the LLM must surface the denial to the user naturally.
    SQLiteDataSource ds = createCountersDatabase();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 0));
    registry.register(new RunMutationQueryTool(ds, 0));

    ApprovalMiddleware approval = new ApprovalMiddleware(30);

    ReactAgent agent =
        ReactAgent.builder()
            .client(client)
            .modelName(MODEL_NAME)
            .toolRegistry(registry)
            .addMiddleware(new LoggingMiddleware())
            .addMiddleware(approval)
            .maxIterations(10)
            .systemPrompt(SYSTEM_PROMPT)
            .build();

    List<AgentEvent> events = new CopyOnWriteArrayList<>();
    ConversationMemory memory = new ConversationMemory();

    ExecutorService approver = Executors.newSingleThreadExecutor();
    Consumer<AgentEvent> listener =
        event -> {
          events.add(event);
          if (event instanceof ApprovalRequest) {
            String rid = ((ApprovalRequest) event).requestId();
            approver.submit(() -> approval.resolve(rid, false));
          }
        };

    try {
      agent.run(
          new AgentInvocation(
              "Delete all rows from the counters table. If you cannot, explain why."),
          memory,
          listener);
    } finally {
      approver.shutdown();
      approver.awaitTermination(5, TimeUnit.SECONDS);
    }

    assertTrue(
        "Expected at least one ApprovalRequest",
        events.stream().anyMatch(e -> e instanceof ApprovalRequest));

    // DB must be untouched.
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM counters")) {
      assertTrue(rs.next());
      assertTrue("Counters rows must survive denied mutation", rs.getInt(1) > 0);
    }

    // LLM should tell the user the operation didn't go through. Loose lexical check to
    // absorb model wording drift.
    String finalAnswer =
        events.stream()
            .filter(e -> e instanceof ContentComplete)
            .map(e -> ((ContentComplete) e).fullText())
            .reduce((a, b) -> b)
            .orElse("")
            .toLowerCase();
    assertTrue(
        "Final answer should indicate the deletion was refused/denied/not executed, got: "
            + finalAnswer,
        finalAnswer.contains("den")
            || finalAnswer.contains("reject")
            || finalAnswer.contains("not ")
            || finalAnswer.contains("refus")
            || finalAnswer.contains("unable")
            || finalAnswer.contains("could not"));
  }

  // --- Helpers ---

  private SQLiteDataSource createSalesDatabase() {
    SQLiteDataSource ds = createDataSource();
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE products ("
              + "id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
              + "category TEXT NOT NULL, price REAL NOT NULL)");
      stmt.execute(
          "INSERT INTO products VALUES "
              + "(1, 'Laptop', 'Electronics', 999.99), "
              + "(2, 'Headphones', 'Electronics', 199.99), "
              + "(3, 'T-Shirt', 'Clothing', 29.99), "
              + "(4, 'Jeans', 'Clothing', 59.99), "
              + "(5, 'Novel', 'Books', 14.99), "
              + "(6, 'Textbook', 'Books', 89.99)");
      stmt.execute(
          "CREATE TABLE orders ("
              + "id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL, "
              + "customer_name TEXT NOT NULL, quantity INTEGER NOT NULL, "
              + "order_date TEXT NOT NULL, "
              + "FOREIGN KEY (product_id) REFERENCES products(id))");
      stmt.execute(
          "INSERT INTO orders VALUES "
              + "(1, 1, 'Alice', 1, '2024-01-15'), "
              + "(2, 2, 'Bob', 2, '2024-01-20'), "
              + "(3, 3, 'Charlie', 3, '2024-02-01'), "
              + "(4, 4, 'Alice', 1, '2024-02-10'), "
              + "(5, 5, 'Bob', 5, '2024-02-15'), "
              + "(6, 1, 'Diana', 1, '2024-03-01'), "
              + "(7, 6, 'Charlie', 2, '2024-03-05'), "
              + "(8, 2, 'Diana', 1, '2024-03-10')");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ds;
  }

  private SQLiteDataSource createNeedleInHaystackDatabase() {
    SQLiteDataSource ds = createDataSource();
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE events ("
              + "id INTEGER PRIMARY KEY, tag TEXT NOT NULL, note TEXT NOT NULL)");
      conn.setAutoCommit(false);
      try (java.sql.PreparedStatement ps =
          conn.prepareStatement("INSERT INTO events VALUES (?, ?, ?)")) {
        // 800 filler rows, guaranteed to blow past ToolResultOffloadMiddleware's 500-line
        // threshold when the LLM issues a SELECT *. Exactly one row carries the NEEDLE tag.
        int needleId = 573;
        for (int i = 1; i <= 800; i++) {
          ps.setInt(1, i);
          if (i == needleId) {
            ps.setString(2, "NEEDLE");
            ps.setString(3, "the-answer-is-42");
          } else {
            ps.setString(2, "FILLER");
            ps.setString(3, "filler-note-" + i);
          }
          ps.addBatch();
        }
        ps.executeBatch();
      }
      conn.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ds;
  }

  private SQLiteDataSource createCountersDatabase() {
    SQLiteDataSource ds = createDataSource();
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)");
      stmt.execute("INSERT INTO counters VALUES ('hits', 0)");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ds;
  }

  private SQLiteDataSource createDataSource() {
    try {
      File tmpFile = File.createTempFile("kyuubi-agent-live-", ".db");
      tmpFile.deleteOnExit();
      tempFiles.add(tmpFile);
      SQLiteDataSource ds = new SQLiteDataSource();
      ds.setUrl("jdbc:sqlite:" + tmpFile.getAbsolutePath());
      return ds;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
