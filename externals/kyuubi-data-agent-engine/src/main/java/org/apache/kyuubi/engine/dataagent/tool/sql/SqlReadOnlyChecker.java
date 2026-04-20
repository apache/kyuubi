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

package org.apache.kyuubi.engine.dataagent.tool.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Lightweight read-only check for the {@code run_select_query} tool.
 *
 * <p><b>Design note — this is a guardrail, not a security boundary.</b> The real approval gate for
 * writes is the separate {@code run_mutation_query} tool; an LLM with DML intent calls that
 * directly, not via a crafted bypass of this check. This checker only catches the "LLM hallucinates
 * and sends a mutation to the read-only tool" case, for which a first-token keyword whitelist is
 * enough. Pulling a full SQL parser into the tool layer for a sanity check isn't warranted.
 *
 * <p>The whitelist is intentionally broad to cover common big-data read-only entry points. If a
 * legitimate read-only keyword is missing, add it here rather than working around it elsewhere.
 */
final class SqlReadOnlyChecker {

  /**
   * Read-only keywords accepted as the first token of a SQL statement.
   *
   * <ul>
   *   <li>{@code SELECT} — standard query
   *   <li>{@code WITH} — CTE-prefixed query
   *   <li>{@code VALUES} — standalone VALUES expression (Trino/Postgres)
   *   <li>{@code TABLE} — Spark/Postgres {@code TABLE x} shorthand for {@code SELECT * FROM x}
   *   <li>{@code FROM} — Hive {@code FROM t SELECT ...} variant
   *   <li>{@code SHOW} — SHOW TABLES / DATABASES / CREATE / PARTITIONS / FUNCTIONS / COLUMNS /
   *       VIEWS / TBLPROPERTIES / INDEXES / GRANT / ROLE / STATS / LOCKS / ...
   *   <li>{@code DESCRIBE} / {@code DESC} — table / function / formatted / extended
   *   <li>{@code EXPLAIN} — query plan inspection
   *   <li>{@code USE} — switch session catalog/database; not data-mutating
   *   <li>{@code LIST} — Spark {@code LIST FILE} / {@code LIST JAR} inspection
   *   <li>{@code HELP} — some engines expose interactive help
   * </ul>
   */
  private static final Set<String> READ_ONLY_KEYWORDS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "SELECT",
                  "WITH",
                  "VALUES",
                  "TABLE",
                  "FROM",
                  "SHOW",
                  "DESCRIBE",
                  "DESC",
                  "EXPLAIN",
                  "USE",
                  "LIST",
                  "HELP")));

  private SqlReadOnlyChecker() {}

  /**
   * Returns true if the SQL's first significant token is a known read-only keyword. Strips leading
   * whitespace and {@code --} / {@code /* *}{@code /} comments before testing.
   */
  static boolean isReadOnly(String sql) {
    if (sql == null) {
      return false;
    }
    String stripped = stripLeadingNoise(sql);
    if (stripped.isEmpty()) {
      return false;
    }
    int end = 0;
    while (end < stripped.length() && Character.isLetter(stripped.charAt(end))) {
      end++;
    }
    if (end == 0) {
      return false;
    }
    String firstToken = stripped.substring(0, end).toUpperCase(Locale.ROOT);
    return READ_ONLY_KEYWORDS.contains(firstToken);
  }

  private static String stripLeadingNoise(String sql) {
    int i = 0;
    int n = sql.length();
    while (i < n) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
      } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        // line comment until newline
        i += 2;
        while (i < n && sql.charAt(i) != '\n') {
          i++;
        }
      } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        // block comment until */
        i += 2;
        while (i + 1 < n && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
          i++;
        }
        if (i + 1 < n) {
          i += 2;
        } else {
          i = n;
        }
      } else {
        break;
      }
    }
    return sql.substring(i);
  }
}
