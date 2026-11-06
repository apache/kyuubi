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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jackson.JacksonOption;
import java.util.Map;

/**
 * Generates JSON Schema from annotated Java classes using Jackson annotations. Used to build the
 * {@code parameters} section of OpenAI function definitions.
 *
 * <p>Backed by <a href="https://github.com/victools/jsonschema-generator">victools
 * jsonschema-generator</a> with its Jackson module, which natively reads {@code @JsonProperty} and
 * {@code @JsonPropertyDescription} annotations.
 */
public class ToolSchemaGenerator {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final SchemaGenerator GENERATOR;

  static {
    JacksonModule jacksonModule = new JacksonModule(JacksonOption.RESPECT_JSONPROPERTY_REQUIRED);
    SchemaGeneratorConfig config =
        new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_7, OptionPreset.PLAIN_JSON)
            .with(jacksonModule)
            .build();
    GENERATOR = new SchemaGenerator(config);
  }

  /** Generate a JSON Schema (as a Map) from the given args class using Jackson annotations. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> generateSchema(Class<?> argsClass) {
    JsonNode schemaNode = GENERATOR.generateSchema(argsClass);
    Map<String, Object> schema = JSON.convertValue(schemaNode, Map.class);
    // Remove $schema key — OpenAI function parameters don't expect it.
    schema.remove("$schema");
    return schema;
  }
}
