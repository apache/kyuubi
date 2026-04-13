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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class TableRefTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  // --- Factory methods ---

  @Test
  public void testOfTableOnly() {
    TableRef ref = TableRef.of("users");
    assertNull(ref.getCatalog());
    assertNull(ref.getSchema());
    assertEquals("users", ref.getTable());
  }

  @Test
  public void testOfSchemaAndTable() {
    TableRef ref = TableRef.of("mydb", "users");
    assertNull(ref.getCatalog());
    assertEquals("mydb", ref.getSchema());
    assertEquals("users", ref.getTable());
  }

  @Test
  public void testOfCatalogSchemaTable() {
    TableRef ref = TableRef.of("hive", "sales", "orders");
    assertEquals("hive", ref.getCatalog());
    assertEquals("sales", ref.getSchema());
    assertEquals("orders", ref.getTable());
  }

  @Test
  public void testNullCatalogAndSchemaAllowed() {
    TableRef ref = TableRef.of(null, null, "t");
    assertNull(ref.getCatalog());
    assertNull(ref.getSchema());
    assertEquals("t", ref.getTable());
  }

  // --- Validation ---

  @Test(expected = IllegalArgumentException.class)
  public void testNullTableThrows() {
    TableRef.of(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTableThrows() {
    TableRef.of("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullTableInThreeArgThrows() {
    TableRef.of("cat", "sch", null);
  }

  @Test
  public void testEmptyCatalogNormalizedToNull() {
    TableRef ref = TableRef.of("", "mydb", "users");
    assertNull(ref.getCatalog());
    assertEquals("mydb", ref.getSchema());
  }

  @Test
  public void testEmptySchemaNormalizedToNull() {
    TableRef ref = TableRef.of("", "users");
    assertNull(ref.getSchema());
  }

  @Test
  public void testEmptyCatalogAndSchemaEqualsNull() {
    assertEquals(TableRef.of(null, null, "t"), TableRef.of("", "", "t"));
  }

  // --- equals / hashCode ---

  @Test
  public void testEqualsSameFields() {
    TableRef a = TableRef.of("hive", "sales", "orders");
    TableRef b = TableRef.of("hive", "sales", "orders");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsDifferentFields() {
    assertNotEquals(TableRef.of("a", "b", "c"), TableRef.of("x", "b", "c"));
    assertNotEquals(TableRef.of("a", "b", "c"), TableRef.of("a", "x", "c"));
    assertNotEquals(TableRef.of("a", "b", "c"), TableRef.of("a", "b", "x"));
  }

  @Test
  public void testEqualsNullFields() {
    assertEquals(TableRef.of("t"), TableRef.of("t"));
    assertNotEquals(TableRef.of("t"), TableRef.of("s", "t"));
  }

  // --- toString ---

  @Test
  public void testToStringTableOnly() {
    assertEquals("TableRef{table='users'}", TableRef.of("users").toString());
  }

  @Test
  public void testToStringFull() {
    String s = TableRef.of("hive", "sales", "orders").toString();
    assertTrue(s.contains("catalog='hive'"));
    assertTrue(s.contains("schema='sales'"));
    assertTrue(s.contains("table='orders'"));
  }

  // --- JSON deserialization ---

  @Test
  public void testDeserializeTableOnly() throws Exception {
    TableRef ref = JSON.readValue("{\"table\":\"users\"}", TableRef.class);
    assertNull(ref.getCatalog());
    assertNull(ref.getSchema());
    assertEquals("users", ref.getTable());
  }

  @Test
  public void testDeserializeSchemaAndTable() throws Exception {
    TableRef ref = JSON.readValue("{\"schema\":\"mydb\",\"table\":\"users\"}", TableRef.class);
    assertNull(ref.getCatalog());
    assertEquals("mydb", ref.getSchema());
    assertEquals("users", ref.getTable());
  }

  @Test
  public void testDeserializeFull() throws Exception {
    TableRef ref =
        JSON.readValue(
            "{\"catalog\":\"hive\",\"schema\":\"sales\",\"table\":\"orders\"}", TableRef.class);
    assertEquals("hive", ref.getCatalog());
    assertEquals("sales", ref.getSchema());
    assertEquals("orders", ref.getTable());
  }

  @Test(expected = Exception.class)
  public void testDeserializeMissingTableThrows() throws Exception {
    JSON.readValue("{\"schema\":\"mydb\"}", TableRef.class);
  }
}
