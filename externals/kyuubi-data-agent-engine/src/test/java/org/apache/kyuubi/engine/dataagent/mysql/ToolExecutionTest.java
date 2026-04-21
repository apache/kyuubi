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

package org.apache.kyuubi.engine.dataagent.mysql;

import static org.junit.Assert.*;

import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunSelectQueryTool;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for ToolRegistry-level behavior and cross-tool workflows against a real MySQL
 * instance. Individual tool behavior is tested in {@link RunSelectQueryTest} and {@link
 * RunMutationQueryTest}; this class covers registry dispatch, timeout, error handling, and
 * multi-step agent workflows.
 */
public class ToolExecutionTest extends WithMySQLContainer {

  @BeforeClass
  public static void setUp() {
    exec(
        "CREATE TABLE IF NOT EXISTS workflow_test ("
            + "id BIGINT PRIMARY KEY, "
            + "name VARCHAR(255), "
            + "department VARCHAR(100)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    exec(
        "INSERT INTO workflow_test VALUES "
            + "(1, 'Alice', 'Engineering'), "
            + "(2, 'Bob', 'Marketing')");
  }

  // ---- Agent workflow: explore → analyze → mutate → verify ----

  @Test
  public void testAgentSchemaExplorationWorkflow() {
    // Step 1: agent explores available tables
    String tables = select("SHOW TABLES");
    assertFalse(tables.startsWith("Error:"));
    assertTrue(tables.contains("workflow_test"));

    // Step 2: agent inspects table structure
    String schema = select("DESCRIBE workflow_test");
    assertFalse(schema.startsWith("Error:"));
    assertTrue(schema.contains("department"));

    // Step 3: agent queries data
    String data = select("SELECT * FROM workflow_test ORDER BY id LIMIT 10");
    assertFalse(data.startsWith("Error:"));
    assertTrue(data.contains("Alice"));
    assertTrue(data.contains("[2 row(s) returned]"));
  }

  @Test
  public void testAgentMutateThenVerify() {
    // Agent inserts a row via mutation tool
    String insertResult =
        mutate("INSERT INTO workflow_test VALUES (100, 'NewHire', 'Engineering')");
    assertTrue(insertResult.contains("1 row(s) affected"));

    // Agent reads it back via select tool to confirm
    String readBack = select("SELECT name FROM workflow_test WHERE id = 100");
    assertFalse(readBack.startsWith("Error:"));
    assertTrue(readBack.contains("NewHire"));

    // Cleanup
    mutate("DELETE FROM workflow_test WHERE id = 100");
  }

  // ---- Registry-level error handling ----

  @Test
  public void testUnknownTool() {
    String result = registry.executeTool("nonexistent_tool", "{\"sql\": \"SELECT 1\"}");
    assertTrue(result.contains("Error:"));
    assertTrue(result.contains("unknown tool"));
  }

  @Test
  public void testMalformedJson() {
    String result = registry.executeTool("run_select_query", "not valid json");
    assertTrue(result.startsWith("Error"));
  }

  // ---- Timeout enforcement ----

  @Test
  public void testQueryTimeoutKillsSlowQuery() {
    // Create a separate registry with a short timeout to test timeout behavior
    ToolRegistry shortTimeoutRegistry = new ToolRegistry(3);
    shortTimeoutRegistry.register(new RunSelectQueryTool(dataSource, 2));

    long start = System.currentTimeMillis();
    String result =
        shortTimeoutRegistry.executeTool("run_select_query", "{\"sql\": \"SELECT SLEEP(60)\"}");
    long elapsed = System.currentTimeMillis() - start;

    // Must not block for 60 seconds
    assertTrue("Should return within 10s, took " + elapsed + "ms", elapsed < 10_000);
    // Either JDBC timeout or ToolRegistry timeout fires
    assertTrue(
        "Expected timeout or interrupted sleep, got: " + result,
        result.contains("timed out") || result.startsWith("Error:") || result.contains("1"));
  }
}
