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

import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for run_mutation_query tool against a real MySQL instance. All calls go through
 * {@link org.apache.kyuubi.engine.dataagent.tool.ToolRegistry}, the same path as the LLM runtime.
 */
public class RunMutationQueryTest extends WithMySQLContainer {

  @Before
  public void setUp() {
    exec("DROP TABLE IF EXISTS mutation_test");
    exec(
        "CREATE TABLE mutation_test ("
            + "id BIGINT PRIMARY KEY, "
            + "name VARCHAR(255), "
            + "age INT"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    exec("INSERT INTO mutation_test VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 28)");
  }

  @Test
  public void testInsert() {
    String result = mutate("INSERT INTO mutation_test VALUES (4, 'Dave', 35)");
    assertTrue(result.contains("1 row(s) affected"));
    assertEquals("4", queryScalar("SELECT COUNT(*) FROM mutation_test"));
  }

  @Test
  public void testBatchInsert() {
    String result =
        mutate("INSERT INTO mutation_test VALUES (10, 'X', 1), (11, 'Y', 2), (12, 'Z', 3)");
    assertTrue(result.contains("3 row(s) affected"));
    assertEquals("6", queryScalar("SELECT COUNT(*) FROM mutation_test"));
  }

  @Test
  public void testUpdate() {
    String result = mutate("UPDATE mutation_test SET age = 99 WHERE age < 30");
    assertTrue(result.contains("2 row(s) affected"));
    assertEquals("2", queryScalar("SELECT COUNT(*) FROM mutation_test WHERE age = 99"));
  }

  @Test
  public void testDelete() {
    String result = mutate("DELETE FROM mutation_test WHERE id = 1");
    assertTrue(result.contains("1 row(s) affected"));
    assertEquals("2", queryScalar("SELECT COUNT(*) FROM mutation_test"));
  }

  @Test
  public void testCreateAndDropTable() {
    String createResult =
        mutate("CREATE TABLE tmp_ddl_test (id INT PRIMARY KEY, v TEXT) ENGINE=InnoDB");
    assertTrue(createResult.contains("executed successfully"));

    String dropResult = mutate("DROP TABLE tmp_ddl_test");
    assertTrue(dropResult.contains("executed successfully"));
  }

  @Test
  public void testCreateIndex() {
    String result = mutate("CREATE INDEX idx_name ON mutation_test (name)");
    assertTrue(result.contains("executed successfully"));
  }

  @Test
  public void testDuplicateKeyError() {
    String result = mutate("INSERT INTO mutation_test VALUES (1, 'Duplicate', 40)");
    assertTrue(result.startsWith("Error:"));
    assertTrue(result.contains("Duplicate entry"));
  }

  @Test
  public void testSyntaxError() {
    String result = mutate("INSRET INTO mutation_test VALUES (99, 'typo', 1)");
    assertTrue(result.startsWith("Error:"));
  }
}
