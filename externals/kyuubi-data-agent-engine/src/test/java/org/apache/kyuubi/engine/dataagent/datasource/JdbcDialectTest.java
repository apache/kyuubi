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

import org.apache.kyuubi.engine.dataagent.datasource.dialect.GenericDialect;
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
  public void testSQLite() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:sqlite:/tmp/test.db");
    assertNotNull(d);
    assertEquals("sqlite", d.datasourceName());
  }

  @Test
  public void testSQLiteCaseInsensitive() {
    assertNotNull(JdbcDialect.fromUrl("JDBC:SQLITE:test.db"));
  }

  @Test
  public void testSQLiteQuoteIdentifier() {
    JdbcDialect sqlite = JdbcDialect.fromUrl("jdbc:sqlite:test.db");
    assertEquals("\"my_table\"", sqlite.quoteIdentifier("my_table"));
    assertEquals("\" \"\"inject\"\" \"", sqlite.quoteIdentifier(" \"inject\" "));
  }

  @Test
  public void testMySQL() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:mysql://localhost:3306");
    assertNotNull(d);
    assertEquals("mysql", d.datasourceName());
    assertEquals("`my_table`", d.quoteIdentifier("my_table"));
  }

  @Test
  public void testUnknownReturnsGenericDialectWithName() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:postgresql://localhost:5432/db");
    assertNotNull(d);
    assertTrue(d instanceof GenericDialect);
    assertEquals("postgresql", d.datasourceName());
  }

  @Test
  public void testGenericDialectQuoteIdentifierUnsupported() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:clickhouse://localhost:8123");
    assertEquals("clickhouse", d.datasourceName());
    try {
      d.quoteIdentifier("col");
      fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      // ok
    }
  }

  // --- qualify tests ---

  @Test
  public void testMySQLQualifySchemaAndTable() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:mysql://localhost:3306");
    assertEquals("`mydb`.`users`", d.qualify(TableRef.of("mydb", "users")));
  }

  @Test
  public void testMySQLQualifyTableOnly() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:mysql://localhost:3306");
    assertEquals("`users`", d.qualify(TableRef.of("users")));
  }

  @Test
  public void testTrinoQualifyFull() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:trino://localhost:9090");
    assertEquals(
        "\"hive\".\"sales\".\"orders\"", d.qualify(TableRef.of("hive", "sales", "orders")));
  }

  @Test
  public void testTrinoQualifySchemaAndTable() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:trino://localhost:9090");
    assertEquals("\"sales\".\"orders\"", d.qualify(TableRef.of("sales", "orders")));
  }

  @Test
  public void testSparkQualifyFull() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:hive2://localhost:10009");
    assertEquals(
        "`spark_catalog`.`default`.`t`", d.qualify(TableRef.of("spark_catalog", "default", "t")));
  }

  @Test
  public void testSQLiteQualifyTableOnly() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:sqlite:test.db");
    assertEquals("\"t\"", d.qualify(TableRef.of("t")));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGenericQualifyThrows() {
    JdbcDialect d = JdbcDialect.fromUrl("jdbc:clickhouse://localhost:8123");
    d.qualify(TableRef.of("db", "t"));
  }

  @Test
  public void testQualifyWithSpecialCharacters() {
    JdbcDialect mysql = JdbcDialect.fromUrl("jdbc:mysql://localhost:3306");
    assertEquals("`my``db`.`user``s`", mysql.qualify(TableRef.of("my`db", "user`s")));

    JdbcDialect trino = JdbcDialect.fromUrl("jdbc:trino://localhost:9090");
    assertEquals("\"my\"\"schema\".\"tab\"", trino.qualify(TableRef.of("my\"schema", "tab")));
  }

  @Test
  public void testNullReturnsNull() {
    assertNull(JdbcDialect.fromUrl(null));
  }

  @Test
  public void testNonJdbcReturnsNull() {
    assertNull(JdbcDialect.fromUrl("postgresql://localhost"));
  }

  @Test
  public void testMalformedReturnsNull() {
    assertNull(JdbcDialect.fromUrl("jdbc:"));
    assertNull(JdbcDialect.fromUrl("jdbc::"));
  }
}
