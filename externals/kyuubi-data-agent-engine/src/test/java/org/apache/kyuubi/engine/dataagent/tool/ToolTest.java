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

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.models.ChatModel;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionTool;
import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunMutationQueryTool;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunSelectQueryTool;
import org.apache.kyuubi.engine.dataagent.tool.sql.SqlQueryArgs;
import org.junit.After;
import org.junit.Test;
import org.sqlite.SQLiteDataSource;

public class ToolTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private final List<File> tempFiles = new ArrayList<>();

  @After
  public void cleanup() {
    tempFiles.forEach(File::delete);
  }

  // --- Args deserialization ---

  @Test
  public void testSqlQueryArgsDeserializesSql() throws Exception {
    SqlQueryArgs args = JSON.readValue("{\"sql\": \"SELECT 1\"}", SqlQueryArgs.class);
    assertEquals("SELECT 1", args.sql);
  }

  // --- AgentTool metadata ---

  @Test
  public void testRunSelectQueryToolMetadata() {
    RunSelectQueryTool tool = new RunSelectQueryTool(createDataSource(), 30);
    assertEquals("run_select_query", tool.name());
    assertTrue(tool.description().contains("READ-ONLY"));
    assertEquals(SqlQueryArgs.class, tool.argsType());
    assertEquals(ToolRiskLevel.SAFE, tool.riskLevel());
  }

  @Test
  public void testRunMutationQueryToolMetadata() {
    RunMutationQueryTool tool = new RunMutationQueryTool(createDataSource(), 30);
    assertEquals("run_mutation_query", tool.name());
    assertTrue(tool.description().contains("MODIFIES"));
    assertEquals(SqlQueryArgs.class, tool.argsType());
    assertEquals(ToolRiskLevel.DESTRUCTIVE, tool.riskLevel());
  }

  // --- ToolRegistry ---

  @Test
  public void testRegistryBuildsChatCompletionToolSpecs() {
    SQLiteDataSource ds = createDataSource();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 30));
    registry.register(new RunMutationQueryTool(ds, 30));
    assertFalse(registry.isEmpty());

    ChatCompletionCreateParams.Builder builder =
        ChatCompletionCreateParams.builder().model(ChatModel.GPT_4O).addUserMessage("test");
    registry.addToolsTo(builder);
    ChatCompletionCreateParams params = builder.build();

    List<ChatCompletionTool> tools = params.tools().orElse(Collections.emptyList());
    assertEquals(2, tools.size());

    List<String> names = new ArrayList<>();
    tools.forEach(t -> names.add(t.asFunction().function().name()));
    assertTrue("Missing run_select_query", names.contains("run_select_query"));
    assertTrue("Missing run_mutation_query", names.contains("run_mutation_query"));
  }

  @Test
  public void testRegistryExecuteToolDeserializesAndDelegates() {
    SQLiteDataSource ds = createDataSource();
    setupTestTable(ds);
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 30));

    String result =
        registry.executeTool("run_select_query", "{\"sql\": \"SELECT COUNT(*) FROM users\"}");
    assertTrue("Expected count of 3, got: " + result, result.contains("3"));
  }

  @Test
  public void testRegistryReturnsErrorForUnknownTool() {
    ToolRegistry registry = new ToolRegistry(30);
    String result = registry.executeTool("nonexistent", "{}");
    assertTrue(result.startsWith("Error: unknown tool"));
  }

  // --- ToolRegistry timeout enforcement ---

  @Test
  public void testRegistryTimeoutKillsSlowToolCall() {
    ToolRegistry registry = new ToolRegistry(2);
    registry.register(
        new AgentTool<ToolRegistryThreadSafetyTest.DummyArgs>() {
          @Override
          public String name() {
            return "slow_tool";
          }

          @Override
          public String description() {
            return "a tool that sleeps forever";
          }

          @Override
          public Class<ToolRegistryThreadSafetyTest.DummyArgs> argsType() {
            return ToolRegistryThreadSafetyTest.DummyArgs.class;
          }

          @Override
          public String execute(ToolRegistryThreadSafetyTest.DummyArgs args) {
            try {
              Thread.sleep(60_000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return "should not reach here";
          }
        });
    long start = System.currentTimeMillis();
    String result = registry.executeTool("slow_tool", "{}");
    long elapsed = System.currentTimeMillis() - start;
    assertTrue("Should contain timeout error", result.contains("timed out"));
    assertTrue("Should contain tool name", result.contains("slow_tool"));
    assertTrue("Should finish within ~3s (timeout=2s)", elapsed < 5000);
  }

  @Test
  public void testRegistryTimeoutDoesNotAffectFastToolCall() {
    ToolRegistry registry = new ToolRegistry(10);
    registry.register(
        new AgentTool<ToolRegistryThreadSafetyTest.DummyArgs>() {
          @Override
          public String name() {
            return "fast_tool";
          }

          @Override
          public String description() {
            return "instant";
          }

          @Override
          public Class<ToolRegistryThreadSafetyTest.DummyArgs> argsType() {
            return ToolRegistryThreadSafetyTest.DummyArgs.class;
          }

          @Override
          public String execute(ToolRegistryThreadSafetyTest.DummyArgs args) {
            return "fast_result";
          }
        });
    String result = registry.executeTool("fast_tool", "{}");
    assertEquals("fast_result", result);
  }

  @Test
  public void testRegistryToolExceptionReturnsError() {
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(
        new AgentTool<ToolRegistryThreadSafetyTest.DummyArgs>() {
          @Override
          public String name() {
            return "boom";
          }

          @Override
          public String description() {
            return "always fails";
          }

          @Override
          public Class<ToolRegistryThreadSafetyTest.DummyArgs> argsType() {
            return ToolRegistryThreadSafetyTest.DummyArgs.class;
          }

          @Override
          public String execute(ToolRegistryThreadSafetyTest.DummyArgs args) {
            throw new RuntimeException("intentional failure");
          }
        });
    String result = registry.executeTool("boom", "{}");
    assertTrue(result.startsWith("Error executing boom"));
    assertTrue(result.contains("intentional failure"));
  }

  @Test
  public void testRegistryGetRiskLevelForUnknownToolReturnsSafe() {
    ToolRegistry registry = new ToolRegistry(30);
    assertEquals(ToolRiskLevel.SAFE, registry.getRiskLevel("nonexistent"));
  }

  @Test
  public void testRegistryGetRiskLevelForRegisteredTool() {
    SQLiteDataSource ds = createDataSource();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 30));
    registry.register(new RunMutationQueryTool(ds, 30));
    assertEquals(ToolRiskLevel.SAFE, registry.getRiskLevel("run_select_query"));
    assertEquals(ToolRiskLevel.DESTRUCTIVE, registry.getRiskLevel("run_mutation_query"));
  }

  @Test
  public void testRegistryInvalidJsonReturnsError() {
    SQLiteDataSource ds = createDataSource();
    ToolRegistry registry = new ToolRegistry(30);
    registry.register(new RunSelectQueryTool(ds, 30));
    String result = registry.executeTool("run_select_query", "not valid json");
    assertTrue(result.startsWith("Error executing"));
  }

  // --- DataSource isolation ---

  @Test
  public void testDifferentDataSourcesAreIsolated() {
    SQLiteDataSource ds1 = createDataSource();
    SQLiteDataSource ds2 = createDataSource();
    setupTable(ds1, "CREATE TABLE t1 (x TEXT)", "INSERT INTO t1 VALUES ('from-ds1')");
    setupTable(ds2, "CREATE TABLE t2 (y TEXT)", "INSERT INTO t2 VALUES ('from-ds2')");

    ToolRegistry reg1 = new ToolRegistry(30);
    reg1.register(new RunSelectQueryTool(ds1, 30));

    ToolRegistry reg2 = new ToolRegistry(30);
    reg2.register(new RunSelectQueryTool(ds2, 30));

    assertTrue(
        reg1.executeTool("run_select_query", "{\"sql\": \"SELECT x FROM t1\"}")
            .contains("from-ds1"));
    assertTrue(
        reg2.executeTool("run_select_query", "{\"sql\": \"SELECT y FROM t2\"}")
            .contains("from-ds2"));

    // ds1 does not have t2
    assertTrue(
        reg1.executeTool("run_select_query", "{\"sql\": \"SELECT * FROM t2\"}").contains("Error:"));
    // ds2 does not have t1
    assertTrue(
        reg2.executeTool("run_select_query", "{\"sql\": \"SELECT * FROM t1\"}").contains("Error:"));
  }

  // --- Helpers ---

  private SQLiteDataSource createDataSource() {
    try {
      File tmpFile = File.createTempFile("kyuubi-tool-test-", ".db");
      tmpFile.deleteOnExit();
      tempFiles.add(tmpFile);
      SQLiteDataSource ds = new SQLiteDataSource();
      ds.setUrl("jdbc:sqlite:" + tmpFile.getAbsolutePath());
      return ds;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void setupTestTable(SQLiteDataSource ds) {
    setupTable(
        ds,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)");
  }

  private void setupTable(SQLiteDataSource ds, String ddl, String dml) {
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(ddl);
      stmt.execute(dml);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
