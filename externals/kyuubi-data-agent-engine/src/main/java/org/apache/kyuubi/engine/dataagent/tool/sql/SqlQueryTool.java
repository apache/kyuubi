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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.kyuubi.engine.dataagent.tool.AgentTool;
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tool for executing SQL statements against the database. */
public class SqlQueryTool implements AgentTool<SqlQueryArgs> {

  private static final Logger LOG = LoggerFactory.getLogger(SqlQueryTool.class);

  /** Fallback used only by the CLI entry point; server-side reads from KyuubiConf. */
  private static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 300;

  private static final int MAX_ROWS_HARD_LIMIT = 1000;
  private static final int MAX_OUTPUT_CHARS = 65536;
  private static final int MAX_CELL_CHARS = 500;

  private final DataSource dataSource;
  private final int queryTimeoutSeconds;

  public SqlQueryTool(DataSource dataSource) {
    this(dataSource, DEFAULT_QUERY_TIMEOUT_SECONDS);
  }

  public SqlQueryTool(DataSource dataSource, int queryTimeoutSeconds) {
    this.dataSource = dataSource;
    this.queryTimeoutSeconds = queryTimeoutSeconds;
  }

  @Override
  public String name() {
    return "sql_query";
  }

  @Override
  public String description() {
    return "Execute a SQL statement against the database and return the results. "
        + "Supports all SQL statements including SELECT, INSERT, UPDATE, DELETE, DDL, etc. "
        + "Parameter 'sql' is required.";
  }

  @Override
  public ToolRiskLevel riskLevel() {
    return ToolRiskLevel.DESTRUCTIVE;
  }

  @Override
  public Class<SqlQueryArgs> argsType() {
    return SqlQueryArgs.class;
  }

  @Override
  public String execute(SqlQueryArgs args) {
    String sql = args.sql != null ? args.sql.trim() : "";
    if (sql.isEmpty()) {
      return "Error: 'sql' parameter is required.";
    }

    // Strip markdown code block if present
    sql = stripMarkdown(sql);

    int maxRows =
        (args.maxRows != null && args.maxRows > 0)
            ? Math.min(args.maxRows, MAX_ROWS_HARD_LIMIT)
            : 100;

    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.setMaxRows(maxRows);
      stmt.setQueryTimeout(queryTimeoutSeconds);
      boolean hasResultSet = stmt.execute(sql);
      if (hasResultSet) {
        try (ResultSet rs = stmt.getResultSet()) {
          return truncateOutput(formatResult(rs));
        }
      } else {
        int updateCount = stmt.getUpdateCount();
        return "[Statement executed successfully. " + updateCount + " row(s) affected]";
      }
    } catch (Exception e) {
      LOG.warn("SQL execution error", e);
      String msg = extractRootCause(e);
      return "Error: SQL execution failed. " + msg;
    }
  }

  private String formatResult(ResultSet rs) throws Exception {
    ResultSetMetaData meta = rs.getMetaData();
    int colCount = meta.getColumnCount();

    StringBuilder sb = new StringBuilder();

    // Header
    sb.append("| ");
    for (int i = 1; i <= colCount; i++) {
      if (i > 1) sb.append(" | ");
      sb.append(meta.getColumnName(i));
    }
    sb.append(" |\n");

    // Separator
    sb.append("|");
    for (int i = 1; i <= colCount; i++) {
      sb.append(" --- |");
    }
    sb.append("\n");

    // Rows
    int rowCount = 0;
    while (rs.next()) {
      sb.append("| ");
      for (int i = 1; i <= colCount; i++) {
        if (i > 1) sb.append(" | ");
        String val = rs.getString(i);
        if (val != null) {
          if (val.length() > MAX_CELL_CHARS) {
            val = val.substring(0, MAX_CELL_CHARS) + "...";
          }
          sb.append(val.replace("|", "\\|"));
        } else {
          sb.append("NULL");
        }
      }
      sb.append(" |\n");
      rowCount++;
    }

    sb.append("\n[").append(rowCount).append(" row(s) returned]");
    return sb.toString();
  }

  private static String truncateOutput(String output) {
    if (output.length() <= MAX_OUTPUT_CHARS) {
      return output;
    }
    return output.substring(0, MAX_OUTPUT_CHARS)
        + "\n\n[Output truncated at "
        + MAX_OUTPUT_CHARS
        + " characters]";
  }

  /**
   * Walk the exception cause chain to find the root cause message, then truncate to a single-line
   * summary so the LLM can diagnose the problem without a full stack trace.
   */
  private static String extractRootCause(Exception e) {
    Throwable root = e;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    String msg = root.getMessage();
    if (msg == null) {
      return root.getClass().getSimpleName();
    }
    // Take only the first line — Spark errors often include the full query plan after a newline.
    int newline = msg.indexOf('\n');
    if (newline > 0) {
      msg = msg.substring(0, newline);
    }
    // Cap length so it doesn't blow up the tool result.
    if (msg.length() > 500) {
      msg = msg.substring(0, 500) + "...";
    }
    return msg;
  }

  /** Strip markdown code fences (``` or ```sql etc.) wrapping the SQL. */
  static String stripMarkdown(String sql) {
    String trimmed = sql.trim();
    if (trimmed.startsWith("```")) {
      String[] lines = trimmed.split("\n");
      StringBuilder cleaned = new StringBuilder();
      for (int i = 1; i < lines.length; i++) {
        if (!lines[i].trim().startsWith("```")) {
          cleaned.append(lines[i]).append("\n");
        }
      }
      return cleaned.toString().trim();
    }
    return sql;
  }
}
