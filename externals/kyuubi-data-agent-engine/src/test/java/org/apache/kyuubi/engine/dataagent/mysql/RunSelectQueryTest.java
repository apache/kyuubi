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

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for run_select_query tool against a real MySQL instance. All calls go through
 * {@link org.apache.kyuubi.engine.dataagent.tool.ToolRegistry}, the same path as the LLM runtime.
 */
public class RunSelectQueryTest extends WithMySQLContainer {

  @BeforeClass
  public static void setUp() {
    exec(
        "CREATE TABLE IF NOT EXISTS select_test ("
            + "id BIGINT PRIMARY KEY, "
            + "name VARCHAR(255), "
            + "price DECIMAL(27,9), "
            + "birth_date DATE, "
            + "created_at DATETIME, "
            + "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            + "active BOOLEAN, "
            + "score DOUBLE, "
            + "status ENUM('ACTIVE','INACTIVE','PENDING')"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

    exec(
        "INSERT INTO select_test (id, name, price, birth_date, created_at, active, score, status) VALUES "
            + "(1, 'Alice', 99.123456789, '1990-01-15', '2024-06-01 10:30:00', true, 88.5, 'ACTIVE'), "
            + "(2, 'Bob', 0.000000001, '1985-12-31', '2024-06-02 14:00:00', false, 72.3, 'INACTIVE'), "
            + "(3, NULL, NULL, NULL, NULL, NULL, NULL, 'PENDING')");
  }

  @Test
  public void testSimpleSelect() {
    String result = select("SELECT id, name FROM select_test ORDER BY id LIMIT 10");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("Alice"));
    assertTrue(result.contains("Bob"));
    assertTrue(result.contains("[3 row(s) returned]"));
  }

  @Test
  public void testMySQLTypes() {
    String result = select("SELECT * FROM select_test WHERE id = 1");
    assertFalse(result.startsWith("Error:"));
    assertTrue("DECIMAL value", result.contains("99.123456789"));
    assertTrue("DATE value", result.contains("1990-01-15"));
    assertTrue("DATETIME value", result.contains("2024-06-01"));
    assertTrue("DOUBLE value", result.contains("88.5"));
    assertTrue("ENUM value", result.contains("ACTIVE"));
  }

  @Test
  public void testShowDatabases() {
    String result = select("SHOW DATABASES");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("test_db"));
  }

  @Test
  public void testShowTables() {
    String result = select("SHOW TABLES");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("select_test"));
  }

  @Test
  public void testDescribeTable() {
    String result = select("DESCRIBE select_test");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("id"));
    assertTrue(result.contains("name"));
    assertTrue(result.contains("bigint"));
  }

  @Test
  public void testShowCreateTable() {
    String result = select("SHOW CREATE TABLE select_test");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("`select_test`") || result.contains("select_test"));
  }

  @Test
  public void testCteQuery() {
    String result =
        select(
            "WITH active_users AS (SELECT id, name FROM select_test WHERE active = true) "
                + "SELECT * FROM active_users");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("Alice"));
    assertFalse(result.contains("Bob"));
  }

  @Test
  public void testChineseData() {
    exec(
        "CREATE TABLE IF NOT EXISTS chinese_test (id INT PRIMARY KEY, name VARCHAR(255)) "
            + "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    exec("INSERT IGNORE INTO chinese_test VALUES (1, '张三'), (2, '李四')");

    String result = select("SELECT * FROM chinese_test ORDER BY id");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("张三"));
    assertTrue(result.contains("李四"));
  }

  @Test
  public void testNullRenderedAsNULL() {
    String result = select("SELECT id, name, price FROM select_test WHERE id = 3");
    assertFalse(result.startsWith("Error:"));
    assertTrue(result.contains("NULL"));
  }

  @Test
  public void testRejectsMutationStatement() {
    String result = select("INSERT INTO select_test (id, name) VALUES (999, 'hacker')");
    assertTrue(result.startsWith("Error:"));
    assertTrue(result.contains("read-only"));
    assertTrue(result.contains("run_mutation_query"));
  }

  @Test
  public void testNonexistentTableReturnsUsefulError() {
    String result = select("SELECT * FROM no_such_table LIMIT 1");
    assertTrue(result.startsWith("Error:"));
    assertTrue(result.contains("no_such_table"));
  }

  @Test
  public void testPipeEscapedInMarkdownOutput() {
    exec(
        "CREATE TABLE IF NOT EXISTS pipe_test (id INT PRIMARY KEY, val TEXT) "
            + "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    exec("INSERT IGNORE INTO pipe_test VALUES (1, 'a|b|c')");

    String result = select("SELECT val FROM pipe_test WHERE id = 1");
    assertFalse(result.startsWith("Error:"));
    assertTrue("Pipe should be escaped for markdown table", result.contains("a\\|b\\|c"));
  }
}
