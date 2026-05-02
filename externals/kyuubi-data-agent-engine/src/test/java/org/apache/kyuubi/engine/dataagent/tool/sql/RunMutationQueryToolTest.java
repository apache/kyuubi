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
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sqlite.SQLiteDataSource;

/** Tests for RunMutationQueryTool. Uses real SQLite — no mocks. */
public class RunMutationQueryToolTest {

  private static final int TEST_TIMEOUT_SECONDS = 30;

  private SQLiteDataSource ds;
  private RunMutationQueryTool tool;
  private final List<File> tempFiles = new ArrayList<>();

  @Before
  public void setUp() {
    ds = createDataSource();
    setupTable(ds);
    tool = new RunMutationQueryTool(ds, TEST_TIMEOUT_SECONDS);
  }

  @After
  public void tearDown() {
    tempFiles.forEach(File::delete);
  }

  @Test
  public void testRiskLevelDestructive() {
    assertEquals(ToolRiskLevel.DESTRUCTIVE, tool.riskLevel());
  }

  @Test
  public void testInsert() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "INSERT INTO t VALUES (9999, 'hello')";
    String result = tool.execute(ToolContext.EMPTY, args);
    assertTrue(result.contains("1 row(s) affected"));
  }

  @Test
  public void testUpdate() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "UPDATE t SET v = 'updated' WHERE id = 1";
    assertTrue(tool.execute(ToolContext.EMPTY, args).contains("1 row(s) affected"));
  }

  @Test
  public void testDelete() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "DELETE FROM t WHERE id = 1";
    assertTrue(tool.execute(ToolContext.EMPTY, args).contains("1 row(s) affected"));
  }

  @Test
  public void testCreateTable() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "CREATE TABLE new_t (id INTEGER PRIMARY KEY, v TEXT)";
    assertTrue(tool.execute(ToolContext.EMPTY, args).contains("executed successfully"));
  }

  @Test
  public void testAlsoAcceptsSelect() {
    // Mutation tool does not enforce read-only check; SELECT works fine here.
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT v FROM t WHERE id = 1";
    String result = tool.execute(ToolContext.EMPTY, args);
    assertFalse(result.startsWith("Error:"));
  }

  @Test
  public void testRejectsEmptyAndNullSql() {
    SqlQueryArgs emptyArgs = new SqlQueryArgs();
    emptyArgs.sql = "";
    assertTrue(tool.execute(ToolContext.EMPTY, emptyArgs).startsWith("Error:"));

    SqlQueryArgs nullArgs = new SqlQueryArgs();
    nullArgs.sql = null;
    assertTrue(tool.execute(ToolContext.EMPTY, nullArgs).startsWith("Error:"));
  }

  @Test
  public void testInvalidSqlReturnsError() {
    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "INSERT INTO nonexistent_table VALUES (1)";
    assertTrue(tool.execute(ToolContext.EMPTY, args).startsWith("Error:"));
  }

  // --- Helpers ---

  private SQLiteDataSource createDataSource() {
    try {
      File tmpFile = File.createTempFile("kyuubi-mutation-test-", ".db");
      tmpFile.deleteOnExit();
      tempFiles.add(tmpFile);
      SQLiteDataSource dataSource = new SQLiteDataSource();
      dataSource.setUrl("jdbc:sqlite:" + tmpFile.getAbsolutePath());
      return dataSource;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void setupTable(SQLiteDataSource dataSource) {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
      stmt.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
