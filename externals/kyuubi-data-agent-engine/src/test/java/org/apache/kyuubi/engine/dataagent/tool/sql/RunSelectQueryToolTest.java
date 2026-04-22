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

package org.apache.kyuubi.engine.dataagent.tool.sql;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.kyuubi.engine.dataagent.tool.ToolContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sqlite.SQLiteDataSource;

/** Tests for RunSelectQueryTool. Uses real SQLite — no mocks. */
public class RunSelectQueryToolTest {

  private static final int TEST_TIMEOUT_SECONDS = 30;

  private SQLiteDataSource ds;
  private RunSelectQueryTool tool;
  private final List<File> tempFiles = new ArrayList<>();

  @Before
  public void setUp() {
    ds = createDataSource();
    setupLargeTable(ds);
    tool = new RunSelectQueryTool(ds, TEST_TIMEOUT_SECONDS);
  }

  @After
  public void tearDown() {
    tempFiles.forEach(File::delete);
  }

  // --- Read-only enforcement ---

  @Test
  public void testRejectsInsert() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "INSERT INTO large_table VALUES (9999, 'x')";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertTrue(result.startsWith("Error:"));
    assertTrue(result.contains("read-only"));
    assertTrue(result.contains("run_mutation_query"));
  }

  @Test
  public void testRejectsUpdate() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "UPDATE large_table SET value = 'x' WHERE id = 1";
    assertTrue(tool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
  }

  @Test
  public void testRejectsDelete() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "DELETE FROM large_table WHERE id = 1";
    assertTrue(tool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
  }

  @Test
  public void testRejectsCreateTable() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "CREATE TABLE x (id INT)";
    assertTrue(tool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
  }

  @Test
  public void testAllowsSelect() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id FROM large_table LIMIT 100";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("[100 row(s) returned]"));
  }

  @Test
  public void testAllowsCte() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "WITH cte AS (SELECT id, value FROM large_table LIMIT 5) SELECT * FROM cte";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("row(s)"));
  }

  // --- LIMIT is the LLM's responsibility (no client-side cap) ---

  @Test
  public void testRespectsLimitInSql() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id FROM large_table LIMIT 5";
    assertTrue(tool.execute(args, ToolContext.EMPTY).contains("[5 row(s) returned]"));
  }

  @Test
  public void testNoClientSideCapWhenLimitOmitted() {
    // 1500 rows in table; tool MUST return all of them when no LIMIT is given.
    // Cap discipline is delegated to the LLM via the system prompt.
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id FROM large_table";
    assertTrue(tool.execute(args, ToolContext.EMPTY).contains("[1500 row(s) returned]"));
  }

  // --- Zero-row result ---

  @Test
  public void testZeroRowsResult() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id FROM large_table WHERE id < 0";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("[0 row(s) returned]"));
  }

  // --- Comment handling end-to-end ---

  @Test
  public void testSelectWithLeadingBlockComment() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "/* get count */ SELECT COUNT(*) FROM large_table";
    assertFalse(tool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
  }

  @Test
  public void testRejectsMutationHiddenBehindComment() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "-- looks innocent\nDROP TABLE large_table";
    assertTrue(tool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
    assertTrue(tool.execute(args, ToolContext.EMPTY).contains("read-only"));
  }

  // --- Edge cases ---

  @Test
  public void testRejectsEmptyAndNullSql() {
    SqlQueryArgs emptyArgs = new SqlQueryArgs();
    emptyArgs.sql = "";
    assertTrue(tool.execute(emptyArgs, ToolContext.EMPTY).startsWith("Error:"));

    SqlQueryArgs nullArgs = new SqlQueryArgs();
    nullArgs.sql = null;
    assertTrue(tool.execute(nullArgs, ToolContext.EMPTY).startsWith("Error:"));
  }

  @Test
  public void testRejectsWhitespaceOnlySql() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "   \t\n  ";
    assertTrue(tool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
  }

  @Test
  public void testInvalidSqlReturnsError() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT * FROM nonexistent_table";
    assertTrue(tool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
  }

  // --- Output formatting ---

  @Test
  public void testNullValuesRenderedAsNULL() {
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE nullable_test (id INTEGER, name TEXT)");
      stmt.execute("INSERT INTO nullable_test VALUES (1, NULL)");
      stmt.execute("INSERT INTO nullable_test VALUES (NULL, 'Alice')");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id, name FROM nullable_test ORDER BY ROWID";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertTrue(result.contains("NULL"));
    assertTrue(result.contains("Alice"));
  }

  @Test
  public void testPipeCharacterEscapedInOutput() {
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE pipe_test (val TEXT)");
      stmt.execute("INSERT INTO pipe_test VALUES ('a|b|c')");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT val FROM pipe_test";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertTrue("Pipe should be escaped for markdown table", result.contains("a\\|b\\|c"));
  }

  // --- Error formatting ---

  @Test
  public void testExtractRootCauseFromNestedExceptions() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT * FROM this_table_does_not_exist_at_all";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertTrue(result.startsWith("Error:"));
    assertTrue(result.contains("this_table_does_not_exist_at_all"));
  }

  @Test
  public void testErrorMessageIsConcise() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELEC INVALID SYNTAX HERE !!!";
    String result = tool.execute(args, ToolContext.EMPTY);
    assertTrue(result.startsWith("Error:"));
    long newlines = result.chars().filter(c -> c == '\n').count();
    assertTrue("Error should be concise (<=2 newlines), got " + newlines, newlines <= 2);
  }

  // --- Query timeout ---

  @Test
  public void testCustomQueryTimeout() {
    RunSelectQueryTool customTool = new RunSelectQueryTool(ds, 5);
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT COUNT(*) FROM large_table";
    assertFalse(customTool.execute(args, ToolContext.EMPTY).startsWith("Error:"));
  }

  @Test
  public void testQueryTimeoutReturnsError() throws Exception {
    javax.sql.DataSource slowDs =
        new javax.sql.DataSource() {
          @Override
          public Connection getConnection() throws java.sql.SQLException {
            Connection real = ds.getConnection();
            return (Connection)
                java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {Connection.class},
                    (proxy, method, args) -> {
                      if ("createStatement".equals(method.getName())) {
                        Statement realStmt = real.createStatement();
                        return java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {Statement.class},
                            (p2, m2, a2) -> {
                              if ("execute".equals(m2.getName())) {
                                realStmt.close();
                                real.close();
                                throw new java.sql.SQLTimeoutException(
                                    "Query timed out after 1 seconds");
                              }
                              return m2.invoke(realStmt, a2);
                            });
                      }
                      if ("close".equals(method.getName())) {
                        real.close();
                        return null;
                      }
                      return method.invoke(real, args);
                    });
          }

          @Override
          public Connection getConnection(String u, String p) throws java.sql.SQLException {
            return getConnection();
          }

          @Override
          public java.io.PrintWriter getLogWriter() {
            return null;
          }

          @Override
          public void setLogWriter(java.io.PrintWriter out) {}

          @Override
          public void setLoginTimeout(int seconds) {}

          @Override
          public int getLoginTimeout() {
            return 0;
          }

          @Override
          public java.util.logging.Logger getParentLogger() {
            return null;
          }

          @Override
          public <T> T unwrap(Class<T> iface) {
            return null;
          }

          @Override
          public boolean isWrapperFor(Class<?> iface) {
            return false;
          }
        };

    RunSelectQueryTool timeoutTool = new RunSelectQueryTool(slowDs, 1);
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT * FROM large_table";
    String result = timeoutTool.execute(args, ToolContext.EMPTY);
    assertTrue("Expected error on timeout", result.startsWith("Error:"));
    assertTrue("Expected timeout message", result.contains("timed out"));
  }

  // --- Helpers ---

  private SQLiteDataSource createDataSource() {
    try {
      File tmpFile = File.createTempFile("kyuubi-select-test-", ".db");
      tmpFile.deleteOnExit();
      tempFiles.add(tmpFile);
      SQLiteDataSource dataSource = new SQLiteDataSource();
      dataSource.setUrl("jdbc:sqlite:" + tmpFile.getAbsolutePath());
      return dataSource;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void setupLargeTable(SQLiteDataSource dataSource) {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE large_table (id INTEGER PRIMARY KEY, value TEXT)");
      StringBuilder sb = new StringBuilder();
      for (int i = 1; i <= 1500; i++) {
        if (sb.length() > 0) sb.append(",");
        sb.append("(").append(i).append(", 'row-").append(i).append("')");
        if (i % 500 == 0) {
          stmt.execute("INSERT INTO large_table VALUES " + sb);
          sb.setLength(0);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
