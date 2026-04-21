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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Objects;

/**
 * Immutable reference to a table within a datasource, modeled as a three-level namespace: {@code
 * catalog.schema.table}.
 *
 * <h3>Design intent</h3>
 *
 * <p>Different datasources use different namespace hierarchies: Trino has {@code
 * catalog.schema.table} (3 levels), MySQL has {@code database.table} (2 levels), and SQLite is
 * essentially flat. This class normalises them into a single {@code (catalog?, schema?, table)}
 * triple so that dialect "recipes" (preset analytical tools) can pass table references around
 * without branching on the datasource type.
 *
 * <p>The mapping convention follows the JDBC standard: MySQL's "database" maps to the {@code
 * schema} field (MySQL's JDBC driver reports databases via {@code DatabaseMetaData .getSchemas()}),
 * while {@code catalog} is {@code null}. Trino and Spark use all three levels.
 *
 * <h3>Where instances come from</h3>
 *
 * <ul>
 *   <li><b>Recipe-internal construction</b> — a preset tool enumerates tables via {@code SHOW
 *       TABLES} / {@code information_schema}, then builds {@code TableRef} instances from the
 *       result set to feed into downstream recipe methods.
 *   <li><b>LLM tool parameters</b> — an agent tool can declare a {@code TableRef} field in its args
 *       class; Jackson deserialises the structured JSON ({@code
 *       {"catalog":"hive","schema":"sales","table":"orders"}}) sent by the LLM directly into this
 *       class.
 *   <li><b>Inter-tool handoff</b> — one tool returns {@code List<TableRef>} as part of its result,
 *       and a subsequent tool consumes them.
 * </ul>
 *
 * <h3>How to turn a {@code TableRef} into SQL</h3>
 *
 * <p>Use {@link JdbcDialect#qualify(TableRef)} — it quotes each non-null segment with the
 * dialect-appropriate character and joins them with {@code .}. Do <em>not</em> concatenate the
 * fields manually; that bypasses identifier escaping and risks SQL injection.
 *
 * <h3>Namespace mapping reference</h3>
 *
 * <table>
 *   <tr><th>Datasource</th><th>catalog</th><th>schema</th><th>Example</th></tr>
 *   <tr><td>MySQL</td><td>{@code null}</td><td>database name</td>
 *       <td>{@code TableRef.of("mydb", "users")}</td></tr>
 *   <tr><td>Trino</td><td>connector name</td><td>schema name</td>
 *       <td>{@code TableRef.of("hive", "sales", "orders")}</td></tr>
 *   <tr><td>Spark</td><td>catalog name</td><td>database name</td>
 *       <td>{@code TableRef.of("spark_catalog", "default", "t")}</td></tr>
 *   <tr><td>SQLite</td><td>{@code null}</td><td>{@code null} (or attached db)</td>
 *       <td>{@code TableRef.of("t")}</td></tr>
 * </table>
 */
public final class TableRef {

  @JsonPropertyDescription(
      "Catalog name (e.g. Trino connector or Spark catalog). Omit for MySQL/SQLite.")
  private final String catalog;

  @JsonPropertyDescription(
      "Schema or database name (e.g. MySQL database, Trino schema). Omit if not applicable.")
  private final String schema;

  @JsonPropertyDescription("Table name (required).")
  private final String table;

  @JsonCreator
  private TableRef(
      @JsonProperty("catalog") String catalog,
      @JsonProperty("schema") String schema,
      @JsonProperty(value = "table", required = true) String table) {
    if (table == null || table.isEmpty()) {
      throw new IllegalArgumentException("table must not be null or empty");
    }
    this.catalog = emptyToNull(catalog);
    this.schema = emptyToNull(schema);
    this.table = table;
  }

  /** Create a table reference with only a table name. */
  public static TableRef of(String table) {
    return new TableRef(null, null, table);
  }

  /** Create a table reference with schema (or database) and table. */
  public static TableRef of(String schema, String table) {
    return new TableRef(null, schema, table);
  }

  /** Create a table reference with catalog, schema, and table. */
  public static TableRef of(String catalog, String schema, String table) {
    return new TableRef(catalog, schema, table);
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  private static String emptyToNull(String s) {
    return (s == null || s.isEmpty()) ? null : s;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableRef)) return false;
    TableRef that = (TableRef) o;
    return Objects.equals(catalog, that.catalog)
        && Objects.equals(schema, that.schema)
        && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, schema, table);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TableRef{");
    if (catalog != null) sb.append("catalog='").append(catalog).append("', ");
    if (schema != null) sb.append("schema='").append(schema).append("', ");
    sb.append("table='").append(table).append("'}");
    return sb.toString();
  }
}
