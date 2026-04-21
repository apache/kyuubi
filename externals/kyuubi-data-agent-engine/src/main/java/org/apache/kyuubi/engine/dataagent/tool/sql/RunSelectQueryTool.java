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

import javax.sql.DataSource;
import org.apache.kyuubi.engine.dataagent.tool.AgentTool;
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;

/**
 * Read-only SQL tool. Accepts only statements whose first significant token is a known read-only
 * keyword (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, ...). Mutating statements are rejected with a
 * clear error so the LLM knows to use {@link RunMutationQueryTool} instead.
 */
public class RunSelectQueryTool implements AgentTool<SqlQueryArgs> {

  private final DataSource dataSource;
  private final int queryTimeoutSeconds;

  /**
   * @param dataSource pooled JDBC connection source
   * @param queryTimeoutSeconds value for {@code Statement.setQueryTimeout}, sourced from {@code
   *     kyuubi.engine.data.agent.query.timeout}. This is the JDBC-level inner timeout that lets
   *     Spark/Trino cooperatively cancel a long-running query. The outer wall-clock cap on the
   *     whole tool call is enforced separately by the agent runtime via {@code
   *     kyuubi.engine.data.agent.tool.call.timeout}.
   */
  public RunSelectQueryTool(DataSource dataSource, int queryTimeoutSeconds) {
    this.dataSource = dataSource;
    this.queryTimeoutSeconds = queryTimeoutSeconds;
  }

  @Override
  public String name() {
    return "run_select_query";
  }

  @Override
  public String description() {
    return "Execute a READ-ONLY SQL query and return the results. "
        + "Accepts SELECT, WITH (CTE), SHOW, DESCRIBE, EXPLAIN, USE, VALUES, TABLE, LIST. "
        + "REJECTS any statement that modifies data or schema (INSERT, UPDATE, DELETE, MERGE, "
        + "CREATE, DROP, ALTER, TRUNCATE, GRANT, ...). "
        + "For mutating statements, use the run_mutation_query tool instead.";
  }

  @Override
  public ToolRiskLevel riskLevel() {
    return ToolRiskLevel.SAFE;
  }

  @Override
  public Class<SqlQueryArgs> argsType() {
    return SqlQueryArgs.class;
  }

  @Override
  public String execute(SqlQueryArgs args) {
    String sql = args.sql;
    if (sql == null || sql.trim().isEmpty()) {
      return "Error: 'sql' parameter is required.";
    }
    if (!SqlReadOnlyChecker.isReadOnly(sql)) {
      return "Error: run_select_query only accepts read-only statements "
          + "(SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, USE, VALUES, TABLE, LIST). "
          + "Use run_mutation_query for INSERT/UPDATE/DELETE/DDL statements.";
    }
    return SqlExecutor.execute(dataSource, sql, queryTimeoutSeconds);
  }
}
