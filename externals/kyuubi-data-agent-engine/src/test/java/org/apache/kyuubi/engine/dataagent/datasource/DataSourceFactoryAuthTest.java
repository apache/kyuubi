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

import static org.junit.Assert.*;

import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Test;

/** Tests for DataSourceFactory. Uses real SQLite. */
public class DataSourceFactoryAuthTest {

  private DataSource ds;
  private File tmpFile;

  @After
  public void tearDown() {
    if (ds instanceof HikariDataSource) {
      ((HikariDataSource) ds).close();
    }
    if (tmpFile != null) {
      tmpFile.delete();
    }
  }

  @Test
  public void testCreateWithUrl() throws Exception {
    tmpFile = File.createTempFile("kyuubi-ds-test-", ".db");
    tmpFile.deleteOnExit();

    ds = DataSourceFactory.create("jdbc:sqlite:" + tmpFile.getAbsolutePath());
    assertNotNull(ds);

    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE t (id INTEGER)");
      stmt.execute("INSERT INTO t VALUES (1)");
      try (ResultSet rs = stmt.executeQuery("SELECT id FROM t")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  public void testCreateWithUserOnly() throws Exception {
    tmpFile = File.createTempFile("kyuubi-ds-test-", ".db");
    tmpFile.deleteOnExit();

    ds = DataSourceFactory.create("jdbc:sqlite:" + tmpFile.getAbsolutePath(), "testuser");
    assertNotNull(ds);
    assertTrue(ds instanceof HikariDataSource);
    assertEquals("testuser", ((HikariDataSource) ds).getUsername());
  }

  @Test
  public void testCreateWithUserAndPassword() throws Exception {
    tmpFile = File.createTempFile("kyuubi-ds-test-", ".db");
    tmpFile.deleteOnExit();

    ds = DataSourceFactory.create("jdbc:sqlite:" + tmpFile.getAbsolutePath(), "user", "pass123");
    assertNotNull(ds);
    HikariDataSource hds = (HikariDataSource) ds;
    assertEquals("user", hds.getUsername());
    assertEquals("pass123", hds.getPassword());
  }

  @Test
  public void testCreateWithNullAndEmptyCredentials() throws Exception {
    tmpFile = File.createTempFile("kyuubi-ds-test-", ".db");
    tmpFile.deleteOnExit();

    // null user/password — should not set username/password on config
    ds = DataSourceFactory.create("jdbc:sqlite:" + tmpFile.getAbsolutePath(), null, null);
    assertNotNull(ds);

    // empty strings — treated same as null
    DataSource ds2 = DataSourceFactory.create("jdbc:sqlite:" + tmpFile.getAbsolutePath(), "", "");
    assertNotNull(ds2);
    ((HikariDataSource) ds2).close();
  }

  @Test
  public void testMultipleDataSourcesIsolated() throws Exception {
    File tmpFile1 = File.createTempFile("kyuubi-ds-test1-", ".db");
    File tmpFile2 = File.createTempFile("kyuubi-ds-test2-", ".db");
    tmpFile1.deleteOnExit();
    tmpFile2.deleteOnExit();

    DataSource ds1 = DataSourceFactory.create("jdbc:sqlite:" + tmpFile1.getAbsolutePath());
    DataSource ds2 = DataSourceFactory.create("jdbc:sqlite:" + tmpFile2.getAbsolutePath());

    try (Connection c1 = ds1.getConnection();
        Statement s1 = c1.createStatement()) {
      s1.execute("CREATE TABLE t1 (v TEXT)");
      s1.execute("INSERT INTO t1 VALUES ('from-ds1')");
    }
    try (Connection c2 = ds2.getConnection();
        Statement s2 = c2.createStatement()) {
      s2.execute("CREATE TABLE t2 (v TEXT)");
      s2.execute("INSERT INTO t2 VALUES ('from-ds2')");
    }

    // Verify isolation
    try (Connection c1 = ds1.getConnection();
        Statement s1 = c1.createStatement();
        ResultSet rs = s1.executeQuery("SELECT v FROM t1")) {
      assertTrue(rs.next());
      assertEquals("from-ds1", rs.getString(1));
    }

    ((HikariDataSource) ds1).close();
    ((HikariDataSource) ds2).close();
    tmpFile1.delete();
    tmpFile2.delete();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullJdbcUrlThrows() {
    DataSourceFactory.create(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyJdbcUrlThrows() {
    DataSourceFactory.create("");
  }
}
