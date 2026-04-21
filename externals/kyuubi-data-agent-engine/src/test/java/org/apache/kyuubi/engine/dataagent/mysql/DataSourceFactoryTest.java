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

import static org.junit.Assert.*;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.kyuubi.engine.dataagent.datasource.DataSourceFactory;
import org.junit.Test;

/** Integration tests for {@link DataSourceFactory} against a real MySQL instance. */
public class DataSourceFactoryTest extends WithMySQLContainer {

  @Test
  public void testCreateWithUserPassword() throws Exception {
    DataSource ds =
        DataSourceFactory.create(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    } finally {
      if (ds instanceof HikariDataSource) {
        ((HikariDataSource) ds).close();
      }
    }
  }

  @Test
  public void testCreateWithWrongPassword() {
    DataSource ds = DataSourceFactory.create(mysql.getJdbcUrl(), "root", "wrong_password");
    try (Connection conn = ds.getConnection()) {
      fail("Expected exception for wrong password");
    } catch (Exception e) {
      // HikariCP wraps MySQL auth error — just verify we get an exception.
      // The exception chain should contain the MySQL access-denied message.
      String fullMsg = getFullExceptionMessage(e);
      assertTrue(
          "Expected auth error, got: " + fullMsg,
          fullMsg.contains("Access denied") || fullMsg.contains("password"));
    } finally {
      if (ds instanceof HikariDataSource) {
        ((HikariDataSource) ds).close();
      }
    }
  }

  private static String getFullExceptionMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    while (t != null) {
      if (t.getMessage() != null) {
        sb.append(t.getMessage()).append(" | ");
      }
      t = t.getCause();
    }
    return sb.toString();
  }

  @Test
  public void testConnectionPoolReuse() throws Exception {
    DataSource ds =
        DataSourceFactory.create(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    try {
      // Acquire and release 5 connections sequentially; pool should handle this fine.
      for (int i = 0; i < 5; i++) {
        try (Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT " + i)) {
          assertTrue(rs.next());
          assertEquals(i, rs.getInt(1));
        }
      }
    } finally {
      if (ds instanceof HikariDataSource) {
        ((HikariDataSource) ds).close();
      }
    }
  }
}
