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
import org.apache.kyuubi.engine.dataagent.tool.sql.SqlQueryArgs;
import org.apache.kyuubi.engine.dataagent.tool.sql.SqlQueryTool;
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
  public void testSqlQueryArgsDeserializesWithAllFields() throws Exception {
    SqlQueryArgs args =
        JSON.readValue("{\"sql\": \"SELECT 1\", \"maxRows\": 50}", SqlQueryArgs.class);
    assertEquals("SELECT 1", args.sql);
    assertEquals(Integer.valueOf(50), args.maxRows);
  }

  @Test
  public void testSqlQueryArgsUsesDefaultMaxRows() throws Exception {
    SqlQueryArgs args = JSON.readValue("{\"sql\": \"SELECT 1\"}", SqlQueryArgs.class);
    assertEquals("SELECT 1", args.sql);
    assertEquals(Integer.valueOf(100), args.maxRows);
  }

  // --- AgentTool metadata ---

  @Test
  public void testSqlQueryToolMetadata() {
    SqlQueryTool tool = new SqlQueryTool(createDataSource());
    assertEquals("sql_query", tool.name());
    assertTrue(tool.description().contains("SQL"));
    assertEquals(SqlQueryArgs.class, tool.argsType());
  }

  // --- ToolRegistry ---

  @Test
  public void testRegistryBuildsChatCompletionToolSpecs() {
    SQLiteDataSource ds = createDataSource();
    ToolRegistry registry = new ToolRegistry();
    registry.register(new SqlQueryTool(ds));
    assertFalse(registry.isEmpty());

    ChatCompletionCreateParams.Builder builder =
        ChatCompletionCreateParams.builder().model(ChatModel.GPT_4O).addUserMessage("test");
    registry.addToolsTo(builder);
    ChatCompletionCreateParams params = builder.build();

    List<ChatCompletionTool> tools = params.tools().orElse(Collections.emptyList());
    assertEquals(1, tools.size());

    List<String> names = new ArrayList<>();
    tools.forEach(t -> names.add(t.asFunction().function().name()));
    assertTrue("Missing sql_query", names.contains("sql_query"));
  }

  @Test
  public void testRegistryExecuteToolDeserializesAndDelegates() {
    SQLiteDataSource ds = createDataSource();
    setupTestTable(ds);
    ToolRegistry registry = new ToolRegistry();
    registry.register(new SqlQueryTool(ds));

    String result = registry.executeTool("sql_query", "{\"sql\": \"SELECT COUNT(*) FROM users\"}");
    assertTrue("Expected count of 3, got: " + result, result.contains("3"));
  }

  @Test
  public void testRegistryReturnsErrorForUnknownTool() {
    ToolRegistry registry = new ToolRegistry();
    String result = registry.executeTool("nonexistent", "{}");
    assertTrue(result.startsWith("Error: unknown tool"));
  }

  // --- DataSource isolation ---

  @Test
  public void testDifferentDataSourcesAreIsolated() {
    SQLiteDataSource ds1 = createDataSource();
    SQLiteDataSource ds2 = createDataSource();
    setupTable(ds1, "CREATE TABLE t1 (x TEXT)", "INSERT INTO t1 VALUES ('from-ds1')");
    setupTable(ds2, "CREATE TABLE t2 (y TEXT)", "INSERT INTO t2 VALUES ('from-ds2')");

    ToolRegistry reg1 = new ToolRegistry();
    reg1.register(new SqlQueryTool(ds1));

    ToolRegistry reg2 = new ToolRegistry();
    reg2.register(new SqlQueryTool(ds2));

    assertTrue(
        reg1.executeTool("sql_query", "{\"sql\": \"SELECT x FROM t1\"}").contains("from-ds1"));
    assertTrue(
        reg2.executeTool("sql_query", "{\"sql\": \"SELECT y FROM t2\"}").contains("from-ds2"));

    // ds1 does not have t2
    assertTrue(reg1.executeTool("sql_query", "{\"sql\": \"SELECT * FROM t2\"}").contains("Error:"));
    // ds2 does not have t1
    assertTrue(reg2.executeTool("sql_query", "{\"sql\": \"SELECT * FROM t1\"}").contains("Error:"));
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
