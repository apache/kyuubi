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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder for composing system prompts from Markdown resource sections.
 *
 * <p>Prompt resources live under {@code prompts/} on the classpath as {@code .md} files. The base
 * template supports one placeholder:
 *
 * <ul>
 *   <li>{@code {{tool_descriptions}}} — replaced by {@link #toolDescriptions(String)}
 * </ul>
 *
 * <p>A single datasource section ({@code prompts/datasource-{name}.md}) is set via {@link
 * #datasource(String)}. Calling it again <b>replaces</b> the previous datasource section — an agent
 * session talks to exactly one datasource. Free-form text sections are appended after the
 * datasource section.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * String prompt = SystemPromptBuilder.create()
 *     .toolDescriptions(registry.describeTools())
 *     .datasource("spark")
 *     .section("Only query tables in the public schema.")
 *     .build();
 * }</pre>
 */
public final class SystemPromptBuilder {

  private static final String RESOURCE_PREFIX = "prompts/";

  private String base;
  private String toolDescriptions = "";
  private String datasourceSection;
  private final List<String> sections = new ArrayList<>();

  private SystemPromptBuilder() {
    this.base = loadResource("base");
  }

  public static SystemPromptBuilder create() {
    return new SystemPromptBuilder();
  }

  /** Override the base prompt with custom text instead of the default {@code prompts/base.md}. */
  public SystemPromptBuilder base(String base) {
    this.base = base;
    return this;
  }

  /** Set tool descriptions to substitute into the {@code {{tool_descriptions}}} placeholder. */
  public SystemPromptBuilder toolDescriptions(String toolDescriptions) {
    if (toolDescriptions != null) {
      this.toolDescriptions = toolDescriptions;
    }
    return this;
  }

  /**
   * Set the datasource-specific guidelines. Loads {@code prompts/datasource-{name}.md} from the
   * classpath. If the resource does not exist, falls back to a generic "current dialect is X" hint.
   *
   * <p>Calling this method again <b>replaces</b> the previous datasource section.
   */
  public SystemPromptBuilder datasource(String name) {
    if (name != null) {
      String lower = name.toLowerCase();
      String content = loadResourceOrNull("datasource-" + lower);
      this.datasourceSection = (content != null) ? content : genericDialectSection(lower);
    }
    return this;
  }

  private static String genericDialectSection(String name) {
    return "## Current SQL dialect: "
        + name
        + "\n\nFollow "
        + name
        + " SQL syntax rules. When unsure about specific syntax, run schema exploration commands"
        + " (e.g. SHOW TABLES, DESCRIBE) to verify before writing the query.";
  }

  /** Append a free-form text section to the prompt. */
  public SystemPromptBuilder section(String text) {
    if (text != null && !text.isEmpty()) {
      sections.add(text);
    }
    return this;
  }

  /** Build the final prompt by resolving placeholders and joining all sections. */
  public String build() {
    String result = base.replace("{{tool_descriptions}}", toolDescriptions);

    StringBuilder sb = new StringBuilder(result);
    sb.append("\n\nToday's date: ").append(LocalDate.now()).append(".");
    if (datasourceSection != null) {
      sb.append("\n\n").append(datasourceSection);
    }
    for (String section : sections) {
      sb.append("\n\n").append(section);
    }
    return sb.toString();
  }

  static String loadResource(String name) {
    String content = loadResourceOrNull(name);
    if (content == null) {
      throw new IllegalArgumentException(
          "Prompt resource not found: " + RESOURCE_PREFIX + name + ".md");
    }
    return content;
  }

  private static String loadResourceOrNull(String name) {
    String path = RESOURCE_PREFIX + name + ".md";
    try (InputStream is = SystemPromptBuilder.class.getClassLoader().getResourceAsStream(path)) {
      if (is == null) {
        return null;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read prompt resource: " + path, e);
    }
  }
}
