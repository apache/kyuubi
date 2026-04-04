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

package org.apache.kyuubi.engine.dataagent.datasource;

import static org.junit.Assert.*;

import org.junit.Test;

public class JdbcDialectTest {

  @Test
  public void testSparkViaHive2() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:hive2://localhost:10009/default");
    assertNotNull(d);
    assertEquals("spark", d.datasourceName());
  }

  @Test
  public void testSparkViaSpark() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:spark://localhost:10009/default");
    assertNotNull(d);
    assertEquals("spark", d.datasourceName());
  }

  @Test
  public void testCaseInsensitive() {
    assertNotNull(JdbcDialect.fromUrl("JDBC:HIVE2://localhost:10009"));
    assertNotNull(JdbcDialect.fromUrl("JDBC:SPARK://localhost:10009"));
  }

  @Test
  public void testQuoteIdentifierBacktick() {
    JdbcDialect spark = JdbcDialect.fromUrl("jdbc:hive2://localhost:10009");
    assertEquals("`my_table`", spark.quoteIdentifier("my_table"));
    assertEquals("` ``inject`` `", spark.quoteIdentifier(" `inject` "));
  }

  @Test
  public void testTrino() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:trino://localhost:9090");
    assertNotNull(d);
    assertEquals("trino", d.datasourceName());
  }

  @Test
  public void testTrinoCaseInsensitive() {
    assertNotNull(JdbcDialect.fromUrl("JDBC:TRINO://localhost:9090"));
  }

  @Test
  public void testTrinoQuoteIdentifier() {
    JdbcDialect trino = JdbcDialect.fromUrl("jdbc:trino://localhost:9090");
    assertEquals("\"my_table\"", trino.quoteIdentifier("my_table"));
    assertEquals("\" \"\"inject\"\" \"", trino.quoteIdentifier(" \"inject\" "));
  }

  @Test
  public void testSqlite() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:sqlite:/tmp/test.db");
    assertNotNull(d);
    assertEquals("sqlite", d.datasourceName());
  }

  @Test
  public void testSqliteCaseInsensitive() {
    assertNotNull(JdbcDialect.fromUrl("JDBC:SQLITE:test.db"));
  }

  @Test
  public void testSqliteQuoteIdentifier() {
    JdbcDialect sqlite = JdbcDialect.fromUrl("jdbc:sqlite:test.db");
    assertEquals("\"my_table\"", sqlite.quoteIdentifier("my_table"));
    assertEquals("\" \"\"inject\"\" \"", sqlite.quoteIdentifier(" \"inject\" "));
  }

  @Test
  public void testMysql() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:mysql://localhost:3306");
    assertNotNull(d);
    assertEquals("mysql", d.datasourceName());
    assertEquals("`my_table`", d.quoteIdentifier("my_table"));
  }

  @Test
  public void testUnknownFallsBackToMysql() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:unknown://localhost");
    assertNotNull(d);
    assertEquals("mysql", d.datasourceName());
  }

  @Test
  public void testNullReturnsNull() {
    assertNull(JdbcDialect.fromUrl(null));
  }
}
