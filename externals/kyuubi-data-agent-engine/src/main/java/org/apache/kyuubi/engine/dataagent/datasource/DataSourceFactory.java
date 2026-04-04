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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;

/** Factory for creating pooled DataSource instances from JDBC URLs. */
public final class DataSourceFactory {

  private static final int DEFAULT_MAX_POOL_SIZE = 5;

  private DataSourceFactory() {}

  /**
   * Create a pooled DataSource from a JDBC URL. Supports any JDBC driver available on the
   * classpath. For authenticated databases, credentials can be embedded in the URL.
   *
   * @param jdbcUrl the JDBC connection URL
   * @return a HikariCP-backed DataSource
   */
  public static DataSource create(String jdbcUrl) {
    return create(jdbcUrl, null);
  }

  /**
   * Create a pooled DataSource from a JDBC URL with an explicit username. When the data-agent
   * connects back to Kyuubi Server, the username determines the proxy user for the downstream
   * engine (e.g. Spark). Without it, Kyuubi defaults to "anonymous" which typically fails Hadoop
   * impersonation checks.
   *
   * @param jdbcUrl the JDBC connection URL
   * @param user the username for the JDBC connection, may be null
   * @return a HikariCP-backed DataSource
   */
  public static DataSource create(String jdbcUrl, String user) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    if (user != null && !user.isEmpty()) {
      config.setUsername(user);
    }
    config.setMaximumPoolSize(DEFAULT_MAX_POOL_SIZE);
    config.setMinimumIdle(1);
    config.setInitializationFailTimeout(-1);
    config.setPoolName("kyuubi-data-agent");
    return new HikariDataSource(config);
  }
}
