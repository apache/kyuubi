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

package org.apache.kyuubi.engine.dataagent.tool;

import static org.junit.Assert.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import java.util.Map;
import org.apache.kyuubi.engine.dataagent.tool.sql.SqlQueryArgs;
import org.junit.Test;

public class ToolSchemaGeneratorTest {

  // --- Real args classes ---

  @Test
  public void testSqlQueryArgsSchema() {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(SqlQueryArgs.class);
    assertEquals("object", schema.get("type"));

    @SuppressWarnings("unchecked")
    Map<String, Object> props = (Map<String, Object>) schema.get("properties");
    assertNotNull(props);
    assertTrue(props.containsKey("sql"));
    assertTrue(props.containsKey("maxRows"));

    @SuppressWarnings("unchecked")
    Map<String, Object> sqlProp = (Map<String, Object>) props.get("sql");
    assertEquals("string", sqlProp.get("type"));
    assertNotNull("sql should have a description", sqlProp.get("description"));

    @SuppressWarnings("unchecked")
    Map<String, Object> maxRowsProp = (Map<String, Object>) props.get("maxRows");
    assertEquals("integer", maxRowsProp.get("type"));

    @SuppressWarnings("unchecked")
    List<String> required = (List<String>) schema.get("required");
    assertNotNull(required);
    assertTrue(required.contains("sql"));
    assertFalse("maxRows should not be required", required.contains("maxRows"));
  }

  // --- Synthetic test classes to verify type mapping ---

  public static class AllTypesArgs {
    @JsonProperty(required = true)
    @JsonPropertyDescription("a string field")
    public String stringField;

    @JsonPropertyDescription("an int field")
    public int intField;

    @JsonPropertyDescription("an Integer field")
    public Integer integerField;

    @JsonPropertyDescription("a long field")
    public long longField;

    @JsonPropertyDescription("a double field")
    public double doubleField;

    @JsonPropertyDescription("a float field")
    public float floatField;

    @JsonPropertyDescription("a boolean field")
    public boolean booleanField;

    @JsonPropertyDescription("a Boolean field")
    public Boolean booleanWrapperField;
  }

  @Test
  public void testAllPrimitiveTypeMappingsAndAnnotations() {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(AllTypesArgs.class);

    @SuppressWarnings("unchecked")
    Map<String, Object> props = (Map<String, Object>) schema.get("properties");

    // Type mappings
    assertEquals("string", getType(props, "stringField"));
    assertEquals("integer", getType(props, "intField"));
    assertEquals("integer", getType(props, "integerField"));
    assertEquals("integer", getType(props, "longField"));
    assertEquals("number", getType(props, "doubleField"));
    assertEquals("number", getType(props, "floatField"));
    assertEquals("boolean", getType(props, "booleanField"));
    assertEquals("boolean", getType(props, "booleanWrapperField"));

    // Descriptions preserved
    assertEquals("a string field", getDescription(props, "stringField"));
    assertEquals("an int field", getDescription(props, "intField"));
    assertEquals("a boolean field", getDescription(props, "booleanField"));

    // Required fields
    @SuppressWarnings("unchecked")
    List<String> required = (List<String>) schema.get("required");
    assertNotNull(required);
    assertTrue("stringField should be required", required.contains("stringField"));
    assertFalse("intField should not be required", required.contains("intField"));
  }

  // --- Edge cases ---

  public static class EmptyArgs {}

  @Test
  public void testEmptyArgsClass() {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(EmptyArgs.class);
    assertEquals("object", schema.get("type"));
  }

  public static class NoRequiredArgs {
    @JsonPropertyDescription("optional field")
    public String name;
  }

  @Test
  public void testNoRequiredFields() {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(NoRequiredArgs.class);
    @SuppressWarnings("unchecked")
    List<String> required = (List<String>) schema.get("required");
    assertTrue(required == null || required.isEmpty());
  }

  @Test
  public void testSchemaDoesNotContainDollarSchema() {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(AllTypesArgs.class);
    assertFalse("$schema key should be stripped", schema.containsKey("$schema"));
  }

  public static class ArrayArgs {
    @JsonPropertyDescription("tags")
    public String[] tags;
  }

  @Test
  public void testArrayTypeSupported() {
    Map<String, Object> schema = ToolSchemaGenerator.generateSchema(ArrayArgs.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> props = (Map<String, Object>) schema.get("properties");
    assertEquals("array", getType(props, "tags"));
  }

  // --- Helpers ---

  @SuppressWarnings("unchecked")
  private static String getType(Map<String, Object> props, String fieldName) {
    Map<String, Object> prop = (Map<String, Object>) props.get(fieldName);
    assertNotNull("Property " + fieldName + " should exist", prop);
    return (String) prop.get("type");
  }

  @SuppressWarnings("unchecked")
  private static String getDescription(Map<String, Object> props, String fieldName) {
    Map<String, Object> prop = (Map<String, Object>) props.get(fieldName);
    assertNotNull("Property " + fieldName + " should exist", prop);
    return (String) prop.get("description");
  }
}
