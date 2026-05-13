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
package org.apache.kyuubi.engine.jdbc.oracle

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/** Hive JDBC metadata contract: column-name + key-field assertions per Thrift metadata op. */
class OracleMetadataContractSuite extends WithOracleEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = jdbcConnectionUrl

  // Oracle DROP has no IF EXISTS; swallow ORA-00942 by message (errorCode is wrapped away).
  private def tryDrop(statement: java.sql.Statement, ddl: String): Unit = {
    try statement.execute(ddl)
    catch {
      case e: java.sql.SQLException if e.getMessage != null && e.getMessage.contains("ORA-00942") =>
    }
  }

  test("oracle - getTables result has JDBC contract column names") {
    withJdbcStatement() { statement =>
      // Narrow the scan to a fixed schema; full-catalog metadata over a remote Oracle link is
      // tens of thousands of rows. SYS.DUAL is permanent, so no setup/teardown needed.
      val rs = statement.getConnection.getMetaData.getTables(null, "SYS", "DUAL", null)
      try {
        assertJdbcSpecColumnNames(
          rs,
          Set("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"))
        assertTableRow(
          rs,
          "DUAL",
          tableSchema = Some("SYS"),
          tableType = Some("TABLE"))
      } finally rs.close()
    }
  }

  test("oracle - getColumns result has JDBC contract column names and column shape") {
    val schema = externalOracleUser.toUpperCase
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      tryDrop(statement, "DROP TABLE CONTRACT_T")
      statement.execute(
        "CREATE TABLE CONTRACT_T " +
          "(ID INTEGER NOT NULL, NAME VARCHAR2(64), AGE INTEGER, CREATED TIMESTAMP)")
      try {
        val shapeRs = meta.getColumns(null, schema, "CONTRACT_T", null)
        assertJdbcSpecColumnNames(
          shapeRs,
          Set(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "TYPE_NAME",
            "ORDINAL_POSITION",
            "IS_NULLABLE"))
        // Oracle's INTEGER maps to NUMBER(*,0); the JDBC type code returned by ojdbc is
        // driver-specific (NUMERIC vs DECIMAL across versions) - skip DATA_TYPE assertion.
        assertColumnContract(
          shapeRs,
          "CONTRACT_T",
          Seq(
            ExpectedColumn("ID", nullable = Some(false)),
            ExpectedColumn("NAME", nullable = Some(true)),
            ExpectedColumn("AGE", nullable = Some(true)),
            ExpectedColumn("CREATED", nullable = Some(true))),
          tableSchema = Some(schema))
      } finally {
        tryDrop(statement, "DROP TABLE CONTRACT_T")
      }
    }
  }

  test("oracle - getPrimaryKeys returns single-column PK contract") {
    val schema = externalOracleUser.toUpperCase
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      tryDrop(statement, "DROP TABLE PK_T")
      statement.execute(
        "CREATE TABLE PK_T (ID INTEGER NOT NULL CONSTRAINT pk_t_pk PRIMARY KEY, " +
          "NAME VARCHAR2(64))")
      try {
        val rs = meta.getPrimaryKeys(null, schema, "PK_T")
        assertJdbcSpecColumnNames(
          rs,
          Set("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"))
        val rows = ArrayBuffer[(String, Int, String)]()
        try while (rs.next()) {
            rows += ((rs.getString("COLUMN_NAME"), rs.getInt("KEY_SEQ"), rs.getString("PK_NAME")))
          }
        finally rs.close()
        val pk = rows.filter(_._1.equalsIgnoreCase("id"))
        assert(pk.size == 1, s"expected exactly one PK row for id, got $rows")
        assert(pk.head._2 == 1)
        assert(pk.head._3 != null && pk.head._3.nonEmpty)
      } finally {
        tryDrop(statement, "DROP TABLE PK_T")
      }
    }
  }

  test("oracle - getCatalogs/getSchemas/getTableTypes/getTypeInfo/getFunctions result " +
    "schema contract") {
    val schema = externalOracleUser.toUpperCase
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      // Oracle has no JDBC catalogs (rowcount is 0) but the schema must still be JDBC-shaped.
      val cats = meta.getCatalogs
      try assertJdbcSpecColumnNames(cats, Set("TABLE_CAT"))
      finally cats.close()
      // Narrow getSchemas to the connecting user; full-catalog scan is heavy on remote Oracle.
      val schemas = meta.getSchemas(null, schema)
      try assertJdbcSpecColumnNames(schemas, Set("TABLE_SCHEM", "TABLE_CATALOG"))
      finally schemas.close()
      val tt = meta.getTableTypes
      try assertJdbcSpecColumnNames(tt, Set("TABLE_TYPE"))
      finally tt.close()
      val types = meta.getTypeInfo
      try assertJdbcSpecColumnNames(types, Set("TYPE_NAME", "DATA_TYPE", "NULLABLE"))
      finally types.close()
      val fns = meta.getFunctions(null, schema, "%")
      try assertJdbcSpecColumnNames(
          fns,
          Set(
            "FUNCTION_CAT",
            "FUNCTION_SCHEM",
            "FUNCTION_NAME",
            "FUNCTION_TYPE",
            "SPECIFIC_NAME"))
      finally fns.close()
    }
  }

  test("oracle - getCrossReference returns FK row contract") {
    val schema = externalOracleUser.toUpperCase
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      tryDrop(statement, "DROP TABLE FK_CHILD")
      tryDrop(statement, "DROP TABLE FK_PARENT")
      statement.execute("CREATE TABLE FK_PARENT (ID INTEGER NOT NULL PRIMARY KEY)")
      statement.execute(
        "CREATE TABLE FK_CHILD (ID INTEGER NOT NULL PRIMARY KEY, PARENT_ID INTEGER, " +
          "CONSTRAINT fk_parent FOREIGN KEY (PARENT_ID) REFERENCES FK_PARENT(ID))")
      try {
        val rs = meta.getCrossReference(null, schema, "FK_PARENT", null, schema, "FK_CHILD")
        assertJdbcSpecColumnNames(
          rs,
          Set(
            "PKTABLE_NAME",
            "PKCOLUMN_NAME",
            "FKTABLE_NAME",
            "FKCOLUMN_NAME",
            "KEY_SEQ"))
        val rows = ArrayBuffer[(String, String, String, String, Int)]()
        try while (rs.next()) {
            rows += ((
              rs.getString("PKTABLE_NAME"),
              rs.getString("FKTABLE_NAME"),
              rs.getString("PKCOLUMN_NAME"),
              rs.getString("FKCOLUMN_NAME"),
              rs.getInt("KEY_SEQ")))
          }
        finally rs.close()
        val match_ = rows.filter { case (pkT, fkT, pkC, fkC, _) =>
          pkT.equalsIgnoreCase("FK_PARENT") && fkT.equalsIgnoreCase("FK_CHILD") &&
          pkC.equalsIgnoreCase("id") && fkC.equalsIgnoreCase("parent_id")
        }
        assert(
          match_.size == 1,
          s"expected exactly one FK row matching FK_PARENT.id <- FK_CHILD.parent_id, " +
            s"got rows=$rows")
        assert(match_.head._5 == 1)
      } finally {
        tryDrop(statement, "DROP TABLE FK_CHILD")
        tryDrop(statement, "DROP TABLE FK_PARENT")
      }
    }
  }
}
