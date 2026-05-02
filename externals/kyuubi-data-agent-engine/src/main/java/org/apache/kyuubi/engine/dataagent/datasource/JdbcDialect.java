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

import org.apache.kyuubi.engine.dataagent.datasource.dialect.GenericDialect;
import org.apache.kyuubi.engine.dataagent.datasource.dialect.MySQLDialect;
import org.apache.kyuubi.engine.dataagent.datasource.dialect.SQLiteDialect;
import org.apache.kyuubi.engine.dataagent.datasource.dialect.SparkDialect;
import org.apache.kyuubi.engine.dataagent.datasource.dialect.TrinoDialect;

/**
 * SQL dialect abstraction for datasource-specific SQL generation.
 *
 * <p>Each dialect maps to a datasource name used for prompt resource lookup ({@code
 * prompts/datasource-{name}.md}).
 *
 * <h3>Relationship to kyuubi-jdbc-engine's JdbcDialect</h3>
 *
 * <p>This interface is intentionally decoupled from {@code
 * org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect} in the JDBC engine module. That dialect serves
 * the Thrift protocol layer (mapping JDBC results into TRowSets for Kyuubi clients), while this one
 * serves the Data Agent's tool system — providing identifier quoting, qualified name construction
 * ({@link #qualify(TableRef)}), and (in the future) dialect-specific "recipe" SQL for preset
 * analytical tools. The two evolve independently and share no dependency.
 */
public interface JdbcDialect {

  /** Datasource name for prompt resource lookup (e.g. "spark", "trino"). */
  String datasourceName();

  /**
   * Quote an identifier (table/column/database name) using the dialect-appropriate quote character.
   * Escapes any embedded quote characters by doubling them.
   */
  String quoteIdentifier(String identifier);

  /**
   * Build a fully-qualified table name from a {@link TableRef}, quoting each segment with {@link
   * #quoteIdentifier(String)} and joining with {@code .}. Null segments (catalog, schema) are
   * skipped.
   *
   * @param ref the table reference
   * @return the qualified name, e.g. {@code `mydb`.`users`} or {@code "hive"."sales"."orders"}
   */
  default String qualify(TableRef ref) {
    StringBuilder sb = new StringBuilder();
    if (ref.getCatalog() != null) sb.append(quoteIdentifier(ref.getCatalog())).append('.');
    if (ref.getSchema() != null) sb.append(quoteIdentifier(ref.getSchema())).append('.');
    sb.append(quoteIdentifier(ref.getTable()));
    return sb.toString();
  }

  /**
   * Infer the dialect from a JDBC URL.
   *
   * <p>If the URL prefix matches a built-in dialect (spark/trino/mysql/sqlite) the corresponding
   * implementation is returned. Otherwise a {@link GenericDialect} carrying the extracted
   * subprotocol name (e.g. "postgresql", "clickhouse", "oracle") is returned so prompts can still
   * tell the LLM which SQL flavor it is talking to. Returns {@code null} only when the URL is null
   * or not a parseable {@code jdbc:<name>:...} string.
   *
   * @param jdbcUrl the JDBC connection URL
   * @return the matching dialect, or {@code null} if the URL is unparseable
   */
  static JdbcDialect fromUrl(String jdbcUrl) {
    String name = extractSubprotocol(jdbcUrl);
    if (name == null) {
      return null;
    }
    switch (name) {
      case "hive2":
      case "spark":
        return SparkDialect.INSTANCE;
      case "trino":
        return TrinoDialect.INSTANCE;
      case "mysql":
        return MySQLDialect.INSTANCE;
      case "sqlite":
        return SQLiteDialect.INSTANCE;
      default:
        return new GenericDialect(name);
    }
  }

  /**
   * Extract the subprotocol name from a JDBC URL: {@code jdbc:postgresql://host} → {@code
   * "postgresql"}. Returns null if the URL does not start with {@code jdbc:} or has no second
   * colon.
   */
  static String extractSubprotocol(String jdbcUrl) {
    if (jdbcUrl == null) {
      return null;
    }
    String lower = jdbcUrl.toLowerCase();
    if (!lower.startsWith("jdbc:")) {
      return null;
    }
    int end = lower.indexOf(':', 5);
    if (end <= 5) {
      return null;
    }
    return lower.substring(5, end);
  }
}
