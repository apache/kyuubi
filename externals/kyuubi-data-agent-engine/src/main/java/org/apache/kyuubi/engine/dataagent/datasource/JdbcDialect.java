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

/**
 * SQL dialect abstraction for datasource-specific SQL generation.
 *
 * <p>Each dialect maps to a datasource name used for prompt resource lookup ({@code
 * prompts/datasource-{name}.md}).
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
   * Infer the dialect from a JDBC URL.
   *
   * @param jdbcUrl the JDBC connection URL
   * @return the matching dialect, or {@code null} if unrecognized
   */
  static JdbcDialect fromUrl(String jdbcUrl) {
    if (jdbcUrl == null) {
      return null;
    }
    String lower = jdbcUrl.toLowerCase();
    if (lower.startsWith("jdbc:hive2:") || lower.startsWith("jdbc:spark:")) {
      return SparkDialect.INSTANCE;
    }
    if (lower.startsWith("jdbc:trino:")) {
      return TrinoDialect.INSTANCE;
    }
    if (lower.startsWith("jdbc:mysql:")) {
      return MysqlDialect.INSTANCE;
    }
    if (lower.startsWith("jdbc:sqlite:")) {
      return SqliteDialect.INSTANCE;
    }
    // MySQL as fallback for unrecognized JDBC URLs
    return MysqlDialect.INSTANCE;
  }
}
