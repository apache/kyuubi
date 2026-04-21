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

package org.apache.kyuubi.engine.dataagent.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.Statement;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunMutationQueryTool;
import org.apache.kyuubi.engine.dataagent.tool.sql.RunSelectQueryTool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared base class for MySQL integration tests. Starts a MySQL 8.0.32 container once per test
 * class and provides a pooled DataSource.
 */
public abstract class WithMySQLContainer {

  private static final String MYSQL_IMAGE =
      System.getProperty("kyuubi.test.mysql.image", "mysql:8.0.32");

  private static final long TOOL_CALL_TIMEOUT_SECONDS = 30;
  private static final int QUERY_TIMEOUT_SECONDS = 10;
  private static final ObjectMapper JSON = new ObjectMapper();

  protected static MySQLContainer mysql;
  protected static HikariDataSource dataSource;
  protected static ToolRegistry registry;

  @BeforeClass
  public static void startContainer() {
    DockerImageName imageName =
        DockerImageName.parse(MYSQL_IMAGE).asCompatibleSubstituteFor("mysql");
    mysql =
        new MySQLContainer(imageName)
            .withUsername("root")
            .withPassword("kyuubi")
            .withDatabaseName("test_db")
            .withEnv("MYSQL_ROOT_PASSWORD", "kyuubi");
    mysql.start();

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(mysql.getJdbcUrl());
    config.setUsername(mysql.getUsername());
    config.setPassword(mysql.getPassword());
    config.setMaximumPoolSize(5);
    config.setMinimumIdle(1);
    config.setPoolName("kyuubi-mysql-it");
    dataSource = new HikariDataSource(config);

    registry = new ToolRegistry(TOOL_CALL_TIMEOUT_SECONDS);
    registry.register(new RunSelectQueryTool(dataSource, QUERY_TIMEOUT_SECONDS));
    registry.register(new RunMutationQueryTool(dataSource, QUERY_TIMEOUT_SECONDS));
  }

  @AfterClass
  public static void stopContainer() {
    if (registry != null) {
      registry.close();
    }
    if (dataSource != null) {
      dataSource.close();
    }
    if (mysql != null) {
      mysql.stop();
    }
  }

  /** Execute a SQL statement that does not return a result set. */
  protected static void exec(String sql) {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Invoke a tool through the ToolRegistry, same as the LLM runtime does. Builds the JSON args
   * envelope automatically.
   */
  protected static String callTool(String toolName, String sql) {
    ObjectNode args = JSON.createObjectNode();
    args.put("sql", sql);
    try {
      return registry.executeTool(toolName, JSON.writeValueAsString(args));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Shorthand for {@code callTool("run_select_query", sql)}. */
  protected static String select(String sql) {
    return callTool("run_select_query", sql);
  }

  /** Shorthand for {@code callTool("run_mutation_query", sql)}. */
  protected static String mutate(String sql) {
    return callTool("run_mutation_query", sql);
  }

  /** Execute a SQL statement and return the result of the first column of the first row. */
  protected static String queryScalar(String sql) {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery(sql)) {
      if (rs.next()) {
        return rs.getString(1);
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
