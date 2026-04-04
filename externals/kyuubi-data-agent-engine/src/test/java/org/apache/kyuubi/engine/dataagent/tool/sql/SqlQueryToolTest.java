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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sqlite.SQLiteDataSource;

/**
 * Tests for SqlQueryTool focusing on maxRows enforcement, markdown stripping, write operations, and
 * edge cases. Uses real SQLite — no mocks.
 */
public class SqlQueryToolTest {

  private SQLiteDataSource ds;
  private SqlQueryTool tool;
  private final List<File> tempFiles = new ArrayList<>();

  @Before
  public void setUp() {
    ds = createDataSource();
    setupLargeTable(ds);
    tool = new SqlQueryTool(ds);
  }

  @After
  public void tearDown() {
    tempFiles.forEach(File::delete);
  }

  // --- maxRows enforcement ---

  @Test
  public void testMaxRowsDefaultTo100() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id FROM large_table";
    String result = tool.execute(args);
    assertTrue(result.contains("[100 row(s) returned]"));
  }

  @Test
  public void testMaxRowsCustomValue() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id FROM large_table";
    args.maxRows = 5;
    String result = tool.execute(args);
    assertTrue(result.contains("[5 row(s) returned]"));
  }

  @Test
  public void testMaxRowsHardLimitCapsAt1000() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT id FROM large_table";
    args.maxRows = Integer.MAX_VALUE;
    String result = tool.execute(args);
    // We only have 1500 rows but maxRows capped at 1000
    assertTrue(result.contains("[1000 row(s) returned]"));
  }

  @Test
  public void testMaxRowsEdgeCasesDefaultTo100() {
    for (int invalid : new int[] {0, -1}) {
      SqlQueryArgs args = new SqlQueryArgs();
      args.sql = "SELECT id FROM large_table";
      args.maxRows = invalid;
      String result = tool.execute(args);
      assertTrue(
          "maxRows=" + invalid + " should default to 100",
          result.contains("[100 row(s) returned]"));
    }
  }

  // --- Markdown stripping ---

  @Test
  public void testStripMarkdownCodeFence() {
    assertEquals("SELECT 1", SqlQueryTool.stripMarkdown("```sql\nSELECT 1\n```"));
    assertEquals("SELECT 1", SqlQueryTool.stripMarkdown("```\nSELECT 1\n```"));
    assertEquals("SELECT 1", SqlQueryTool.stripMarkdown("SELECT 1"));
  }

  // --- Write operations ---

  @Test
  public void testAllowsInsert() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "INSERT INTO large_table VALUES (9999, 'test-insert')";
    String result = tool.execute(args);
    assertTrue(result.contains("1 row(s) affected"));
  }

  @Test
  public void testAllowsUpdate() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "UPDATE large_table SET value = 'updated' WHERE id = 1";
    String result = tool.execute(args);
    assertTrue(result.contains("1 row(s) affected"));
  }

  @Test
  public void testAllowsDelete() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "DELETE FROM large_table WHERE id = 1";
    String result = tool.execute(args);
    assertTrue(result.contains("1 row(s) affected"));
  }

  @Test
  public void testAllowsCreateTable() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "CREATE TABLE test_write (id INTEGER PRIMARY KEY, value TEXT)";
    String result = tool.execute(args);
    assertTrue(result.contains("executed successfully"));
  }

  @Test
  public void testAllowsWithCTE() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "WITH cte AS (SELECT id, value FROM large_table LIMIT 5) SELECT * FROM cte";
    String result = tool.execute(args);
    assertTrue("CTE query should work", result.contains("row(s)"));
  }

  // --- Schema exploration ---

  @Test
  public void testExecutesShowStyleQueries() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT name FROM sqlite_master WHERE type='table'";
    String result = tool.execute(args);
    assertTrue(result.contains("large_table"));
  }

  // --- Edge cases ---

  @Test
  public void testRejectsEmptyAndNullSql() {
    SqlQueryArgs emptyArgs = new SqlQueryArgs();
    emptyArgs.sql = "";
    assertTrue(tool.execute(emptyArgs).startsWith("Error:"));

    SqlQueryArgs nullArgs = new SqlQueryArgs();
    nullArgs.sql = null;
    assertTrue(tool.execute(nullArgs).startsWith("Error:"));
  }

  @Test
  public void testInvalidSqlReturnsError() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT * FROM nonexistent_table";
    assertTrue(tool.execute(args).startsWith("Error:"));
  }

  // --- Query timeout ---

  @Test
  public void testCustomQueryTimeout() {
    SqlQueryTool customTool = new SqlQueryTool(ds, 5);
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT COUNT(*) FROM large_table";
    String result = customTool.execute(args);
    assertFalse(result.startsWith("Error:"));
  }

  @Test
  public void testQueryTimeoutReturnsError() throws Exception {
    // Create a DataSource that simulates a timeout by throwing SQLTimeoutException
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

    SqlQueryTool timeoutTool = new SqlQueryTool(slowDs, 1);
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT * FROM large_table";
    String result = timeoutTool.execute(args);
    assertTrue("Expected error on timeout", result.startsWith("Error:"));
    assertTrue("Expected timeout message", result.contains("timed out"));
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
    String result = tool.execute(args);
    // First row: id=1, name=NULL; Second row: id=NULL, name=Alice
    assertTrue("NULL values should render as NULL", result.contains("NULL"));
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
    String result = tool.execute(args);
    assertTrue("Pipe should be escaped for markdown table", result.contains("a\\|b\\|c"));
    assertFalse("Unescaped pipe should not appear in data row", result.contains("| a|b|c |"));
  }

  @Test
  public void testLongCellValueTruncated() {
    String longValue = new String(new char[600]).replace('\0', 'x');
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE cell_test (val TEXT)");
      stmt.execute("INSERT INTO cell_test VALUES ('" + longValue + "')");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT val FROM cell_test";
    String result = tool.execute(args);
    assertTrue("Long cell should be truncated with ...", result.contains("..."));
    // The full 600-char value should NOT appear
    assertFalse(result.contains(longValue));
  }

  @Test
  public void testOutputTruncatedAtMaxChars() {
    // Insert enough rows to produce > 64KB of output
    // Each row: "| <id> | <100-char-value> |\n" ~ 110 chars, need ~600 rows
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE big_output (id INTEGER, val TEXT)");
      String padded = new String(new char[100]).replace('\0', 'A');
      StringBuilder sb = new StringBuilder();
      for (int i = 1; i <= 800; i++) {
        if (sb.length() > 0) sb.append(",");
        sb.append("(").append(i).append(", '").append(padded).append("')");
        if (i % 200 == 0) {
          stmt.execute("INSERT INTO big_output VALUES " + sb);
          sb.setLength(0);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT * FROM big_output";
    args.maxRows = 800;
    String result = tool.execute(args);
    assertTrue(
        "Output should be truncated at 65536 chars",
        result.contains("[Output truncated at 65536 characters]"));
    assertTrue("Truncated output should not exceed limit + message", result.length() < 65536 + 100);
  }

  // --- Error formatting ---

  @Test
  public void testExtractRootCauseFromNestedExceptions() {
    // Trigger a real nested exception via invalid SQL
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT * FROM this_table_does_not_exist_at_all";
    String result = tool.execute(args);
    assertTrue(result.startsWith("Error:"));
    // Should contain a useful message, not a generic class name
    assertTrue(
        "Error should mention the table name", result.contains("this_table_does_not_exist_at_all"));
  }

  @Test
  public void testErrorMessageTruncatesMultilineStackTrace() {
    // Trigger a syntax error — SQLite returns single-line errors, but we can verify
    // the error message doesn't contain excessive output
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELEC INVALID SYNTAX HERE !!!";
    String result = tool.execute(args);
    assertTrue(result.startsWith("Error:"));
    // Error should be a single-line summary, not a full stack trace
    long newlines = result.chars().filter(c -> c == '\n').count();
    assertTrue("Error should be concise (<=2 newlines), got " + newlines, newlines <= 2);
  }

  // --- Helpers ---

  private SQLiteDataSource createDataSource() {
    try {
      File tmpFile = File.createTempFile("kyuubi-sqlquery-test-", ".db");
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
      // Insert 1500 rows to test maxRows capping
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
