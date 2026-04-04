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

package org.apache.kyuubi.engine.dataagent.prompt;

import static org.junit.Assert.*;

import java.time.LocalDate;
import org.junit.Test;

public class SystemPromptBuilderTest {

  @Test
  public void testDefaultBuildContainsBaseAndDate() {
    String prompt = SystemPromptBuilder.create().build();
    assertTrue(prompt.contains("data analysis agent"));
    assertTrue(prompt.contains(LocalDate.now().toString()));
  }

  @Test
  public void testPlaceholdersRemovedByDefault() {
    String prompt = SystemPromptBuilder.create().build();
    assertFalse(prompt.contains("{{tool_descriptions}}"));
  }

  @Test
  public void testToolDescriptionsSubstituted() {
    String tools = "- `sql_query`: Execute a SQL statement against the database.";
    String prompt = SystemPromptBuilder.create().toolDescriptions(tools).build();
    assertTrue(prompt.contains("sql_query"));
    assertTrue(prompt.contains("Execute a SQL statement"));
    assertFalse(prompt.contains("{{tool_descriptions}}"));
  }

  @Test
  public void testDatasourceSqlite() {
    String prompt = SystemPromptBuilder.create().datasource("sqlite").build();
    assertTrue(prompt.contains("SQLite SQL compatibility"));
    assertTrue(prompt.contains("JULIANDAY"));
  }

  @Test
  public void testDatasourceSpark() {
    String prompt = SystemPromptBuilder.create().datasource("spark").build();
    assertTrue(prompt.contains("data analysis agent"));
    assertTrue(prompt.contains("Spark SQL"));
  }

  @Test
  public void testWithCustomBase() {
    String prompt = SystemPromptBuilder.create().base("You are a helper.").build();
    assertTrue(prompt.startsWith("You are a helper."));
    assertTrue(prompt.contains(LocalDate.now().toString()));
  }

  @Test
  public void testWithSection() {
    String prompt = SystemPromptBuilder.create().section("Only query public schema.").build();
    assertTrue(prompt.contains("Only query public schema."));
  }

  @Test
  public void testFullComposition() {
    String prompt =
        SystemPromptBuilder.create()
            .toolDescriptions("- `sql_query`: Execute SQL.")
            .datasource("sqlite")
            .datasource("spark")
            .section("Limit all queries to 1000 rows.")
            .build();
    assertTrue(prompt.contains("sql_query"));
    assertTrue(prompt.contains("SQLite SQL compatibility"));
    assertTrue(prompt.contains("Spark SQL"));
    assertTrue(prompt.contains("Limit all queries to 1000 rows."));
    assertTrue(prompt.contains(LocalDate.now().toString()));
  }

  @Test
  public void testUnknownDatasourceIgnored() {
    String withUnknown = SystemPromptBuilder.create().datasource("unknown").build();
    String plain = SystemPromptBuilder.create().build();
    assertEquals(plain, withUnknown);
  }

  @Test
  public void testJdbcUrlUnknownFallsBackToMysql() {
    String prompt = SystemPromptBuilder.create().jdbcUrl("jdbc:derby://localhost/db").build();
    assertTrue(prompt.contains("MySQL"));
  }

  @Test
  public void testJdbcUrlSqliteAppliesDialect() {
    String prompt = SystemPromptBuilder.create().jdbcUrl("jdbc:sqlite:/tmp/test.db").build();
    assertTrue(prompt.contains("SQLite SQL compatibility"));
  }

  @Test
  public void testJdbcUrlHive2AppliesSparkDialectAndEngine() {
    String prompt = SystemPromptBuilder.create().jdbcUrl("jdbc:hive2://localhost:10009").build();
    assertTrue(prompt.contains("Spark SQL"));
  }

  @Test
  public void testJdbcUrlNullFallsBack() {
    String plain = SystemPromptBuilder.create().build();
    assertEquals(plain, SystemPromptBuilder.create().jdbcUrl(null).build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoadResourceThrowsOnMissing() {
    SystemPromptBuilder.loadResource("nonexistent-resource");
  }

  @Test
  public void testNullsIgnored() {
    String plain = SystemPromptBuilder.create().build();
    assertEquals(plain, SystemPromptBuilder.create().datasource(null).build());
    assertEquals(plain, SystemPromptBuilder.create().toolDescriptions(null).build());
    assertEquals(plain, SystemPromptBuilder.create().section(null).build());
    assertEquals(plain, SystemPromptBuilder.create().section("").build());
  }
}
