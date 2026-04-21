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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared JDBC execution + result formatting for SQL tools. Owned by {@link RunSelectQueryTool} and
 * {@link RunMutationQueryTool}; not part of the public tool API.
 */
final class SqlExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SqlExecutor.class);

  private SqlExecutor() {}

  /**
   * Execute the given SQL and return a markdown-formatted result string. Errors are caught and
   * returned as a one-line "Error: ..." message so the LLM can self-correct.
   *
   * <p><b>No client-side row cap.</b> The tool deliberately does not call {@link
   * Statement#setMaxRows(int)}. Result-size discipline is delegated to the LLM via the system
   * prompt, which requires every read query to include an explicit {@code LIMIT} (or equivalent
   * pagination). Adding a hidden client-side cap here would silently truncate the LLM's view of the
   * data and create magic numbers that are impossible to tune across deployments.
   *
   * <p><b>Two-layer timeout model.</b> The {@code timeoutSeconds} arg here is the JDBC inner
   * timeout, set via {@link Statement#setQueryTimeout(int)} and sourced from {@code
   * kyuubi.engine.data.agent.query.timeout}. This is the cooperative path: the driver tells the
   * server (Spark/Trino/...) to cancel the query, and cluster resources are released gracefully.
   * The outer hard wall-clock cap on the whole tool call is enforced separately by the agent
   * runtime via {@code kyuubi.engine.data.agent.tool.call.timeout} — that one fires even if the
   * JDBC driver ignores {@code setQueryTimeout} (some legacy Hive/Spark Thrift drivers do), and is
   * the ungraceful "kill the thread" backstop.
   */
  static String execute(DataSource dataSource, String sql, int timeoutSeconds) {
    if (sql == null || sql.trim().isEmpty()) {
      return "Error: 'sql' parameter is required.";
    }

    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.setQueryTimeout(timeoutSeconds);
      boolean hasResultSet = stmt.execute(sql);
      if (hasResultSet) {
        try (ResultSet rs = stmt.getResultSet()) {
          return formatResult(rs);
        }
      } else {
        return "[Statement executed successfully. " + stmt.getUpdateCount() + " row(s) affected]";
      }
    } catch (Exception e) {
      LOG.warn("SQL execution error", e);
      return "Error: SQL execution failed. " + extractRootCause(e);
    }
  }

  private static String formatResult(ResultSet rs) throws Exception {
    ResultSetMetaData meta = rs.getMetaData();
    int colCount = meta.getColumnCount();
    StringBuilder sb = new StringBuilder();

    sb.append("| ");
    for (int i = 1; i <= colCount; i++) {
      if (i > 1) sb.append(" | ");
      sb.append(meta.getColumnLabel(i));
    }
    sb.append(" |\n|");
    for (int i = 1; i <= colCount; i++) {
      sb.append(" --- |");
    }
    sb.append("\n");

    int rowCount = 0;
    while (rs.next()) {
      sb.append("| ");
      for (int i = 1; i <= colCount; i++) {
        if (i > 1) sb.append(" | ");
        String val = rs.getString(i);
        if (val != null) {
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
    if (msg.length() > 500) {
      msg = msg.substring(0, 500) + "...";
    }
    return msg;
  }
}
