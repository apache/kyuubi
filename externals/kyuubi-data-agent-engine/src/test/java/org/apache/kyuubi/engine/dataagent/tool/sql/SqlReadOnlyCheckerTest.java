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

import static org.junit.Assert.*;

import org.junit.Test;

public class SqlReadOnlyCheckerTest {

  @Test
  public void testAcceptsSelect() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("SELECT 1"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("select * from t"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("  SELECT 1  "));
  }

  @Test
  public void testAcceptsCte() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("WITH cte AS (SELECT id FROM t) SELECT * FROM cte"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("with x as (select 1) select * from x"));
  }

  @Test
  public void testAcceptsShowDescribeExplain() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("SHOW TABLES"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("SHOW CREATE TABLE t"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("SHOW PARTITIONS t"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("DESCRIBE t"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("DESC FORMATTED t"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("EXPLAIN SELECT 1"));
  }

  @Test
  public void testAcceptsBigDataKeywords() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("USE my_db"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("VALUES (1, 2), (3, 4)"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("TABLE my_table"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("FROM t SELECT *"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("LIST FILE"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("LIST JAR"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("HELP"));
  }

  @Test
  public void testRejectsMutations() {
    assertFalse(SqlReadOnlyChecker.isReadOnly("INSERT INTO t VALUES (1)"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("UPDATE t SET x = 1"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("DELETE FROM t"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("MERGE INTO t USING s ON ..."));
    assertFalse(SqlReadOnlyChecker.isReadOnly("CREATE TABLE t (x INT)"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("DROP TABLE t"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("ALTER TABLE t ADD COLUMN y INT"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("TRUNCATE TABLE t"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("GRANT SELECT ON t TO user"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("ANALYZE TABLE t COMPUTE STATISTICS"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("REFRESH TABLE t"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("SET k = v"));
  }

  @Test
  public void testRejectsEmptyAndNull() {
    assertFalse(SqlReadOnlyChecker.isReadOnly(null));
    assertFalse(SqlReadOnlyChecker.isReadOnly(""));
    assertFalse(SqlReadOnlyChecker.isReadOnly("   "));
    assertFalse(SqlReadOnlyChecker.isReadOnly("123"));
  }

  @Test
  public void testStripsLineComments() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("-- a comment\nSELECT 1"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("-- comment 1\n-- comment 2\nSELECT 1"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("-- innocent\nDROP TABLE t"));
  }

  @Test
  public void testStripsBlockComments() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("/* hi */ SELECT 1"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("/* multi\nline */SELECT 1"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("/* sneaky */ DELETE FROM t"));
  }

  @Test
  public void testHandlesUnterminatedBlockComment() {
    assertFalse(SqlReadOnlyChecker.isReadOnly("/* never ends"));
  }

  @Test
  public void testStripsMixedComments() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("/* block */ -- line\nSELECT 1"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("-- line\n/* block */\nSELECT 1"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("/* block */ -- line\nINSERT INTO t VALUES (1)"));
  }

  @Test
  public void testKeywordFollowedByParen() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("SELECT(1)"));
    assertTrue(SqlReadOnlyChecker.isReadOnly("SHOW;"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("INSERT(1, 2)"));
  }

  @Test
  public void testLineCommentAtEndNoNewline() {
    // "SELECT 1 -- comment" without trailing newline — still read-only
    assertTrue(SqlReadOnlyChecker.isReadOnly("SELECT 1 -- comment"));
  }

  @Test
  public void testTabsAndCarriageReturns() {
    assertTrue(SqlReadOnlyChecker.isReadOnly("\t\r\n  SELECT 1"));
    assertFalse(SqlReadOnlyChecker.isReadOnly("\t\r\n  DROP TABLE t"));
  }
}
