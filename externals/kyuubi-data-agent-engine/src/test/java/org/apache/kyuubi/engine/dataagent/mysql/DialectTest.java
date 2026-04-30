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

import org.apache.kyuubi.engine.dataagent.datasource.JdbcDialect;
import org.apache.kyuubi.engine.dataagent.datasource.dialect.MySQLDialect;
import org.apache.kyuubi.engine.dataagent.prompt.SystemPromptBuilder;
import org.apache.kyuubi.engine.dataagent.tool.ToolContext;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunSelectQueryTool;
import org.apache.kyuubi.engine.dataagent.tool.sql.SqlQueryArgs;
import org.junit.BeforeClass;
import org.junit.Test;

/** Integration tests for {@link MySQLDialect} end-to-end with a real MySQL instance. */
public class DialectTest extends WithMySQLContainer {

  private static RunSelectQueryTool selectTool;

  @BeforeClass
  public static void setUp() {
    selectTool = new RunSelectQueryTool(dataSource, 30);
  }

  @Test
  public void testDialectFromUrl() {
    JdbcDialect dialect = JdbcDialect.fromUrl(mysql.getJdbcUrl());
    assertNotNull(dialect);
    assertTrue(dialect instanceof MySQLDialect);
    assertEquals("mysql", dialect.datasourceName());
  }

  @Test
  public void testBacktickQuotingWithReservedWord() {
    JdbcDialect dialect = JdbcDialect.fromUrl(mysql.getJdbcUrl());

    // Create a table with a column named after a MySQL reserved word
    String quotedTable = dialect.quoteIdentifier("order");
    String quotedCol = dialect.quoteIdentifier("select");

    exec("DROP TABLE IF EXISTS " + quotedTable);
    exec(
        "CREATE TABLE "
            + quotedTable
            + " ("
            + "id INT PRIMARY KEY, "
            + quotedCol
            + " VARCHAR(255)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    exec("INSERT INTO " + quotedTable + " VALUES (1, 'value1')");

    SqlQueryArgs args = new SqlQueryArgs();
    args.sql = "SELECT " + quotedCol + " FROM " + quotedTable + " WHERE id = 1";
    String result = selectTool.execute(ToolContext.EMPTY, args);
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("value1"));

    exec("DROP TABLE " + quotedTable);
  }

  @Test
  public void testBacktickEscaping() {
    JdbcDialect dialect = JdbcDialect.fromUrl(mysql.getJdbcUrl());
    // Identifier containing a backtick should be escaped by doubling
    String quoted = dialect.quoteIdentifier("col`name");
    assertEquals("`col``name`", quoted);
  }

  @Test
  public void testPromptBuilderWithMySQLDatasource() {
    JdbcDialect dialect = JdbcDialect.fromUrl(mysql.getJdbcUrl());
    String prompt = SystemPromptBuilder.create().datasource(dialect.datasourceName()).build();
    assertTrue("Prompt should mention mysql dialect", prompt.toLowerCase().contains("mysql"));
  }
}
