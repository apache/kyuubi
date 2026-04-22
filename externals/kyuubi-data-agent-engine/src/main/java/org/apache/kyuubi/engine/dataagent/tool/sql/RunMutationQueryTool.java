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
import org.apache.kyuubi.engine.dataagent.tool.ToolContext;
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;

/**
 * Mutating SQL tool for INSERT/UPDATE/DELETE/MERGE/DDL statements. Marked {@link
 * ToolRiskLevel#DESTRUCTIVE} so the approval layer gates every call. There is no read-only check —
 * passing a SELECT here will execute fine but the LLM will be told via the description to use
 * {@link RunSelectQueryTool} for read paths.
 */
public class RunMutationQueryTool implements AgentTool<SqlQueryArgs> {

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
  public RunMutationQueryTool(DataSource dataSource, int queryTimeoutSeconds) {
    this.dataSource = dataSource;
    this.queryTimeoutSeconds = queryTimeoutSeconds;
  }

  @Override
  public String name() {
    return "run_mutation_query";
  }

  @Override
  public String description() {
    return "Execute a SQL statement that MODIFIES data or schema. "
        + "Use this for INSERT, UPDATE, DELETE, MERGE, CREATE, DROP, ALTER, TRUNCATE, GRANT, etc. "
        + "REQUIRES USER APPROVAL before execution. "
        + "For read-only queries (SELECT/SHOW/DESCRIBE/EXPLAIN), use run_select_query instead.";
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
  public String execute(SqlQueryArgs args, ToolContext ctx) {
    return SqlExecutor.execute(dataSource, args.sql, queryTimeoutSeconds);
  }
}
