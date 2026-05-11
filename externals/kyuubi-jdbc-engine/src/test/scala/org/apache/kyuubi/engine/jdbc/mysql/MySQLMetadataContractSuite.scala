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
package org.apache.kyuubi.engine.jdbc.mysql

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/** Hive JDBC metadata contract: column-name + key-field assertions per Thrift metadata op. */
class MySQLMetadataContractSuite extends WithMySQLEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = jdbcConnectionUrl

  test("mysql - getTables result has JDBC contract column names") {
    withJdbcStatement() { statement =>
      statement.execute("create database if not exists kyuubi_meta_db")
      statement.execute("drop table if exists kyuubi_meta_db.contract_probe")
      statement.execute(
        "create table kyuubi_meta_db.contract_probe (id BIGINT) ENGINE=InnoDB")
      try {
        val rs = statement.getConnection.getMetaData
          .getTables("kyuubi_meta_db", null, "contract_probe", null)
        try {
          assertJdbcSpecColumnNames(
            rs,
            Set("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"))
          assertTableRow(
            rs,
            "contract_probe",
            tableCatalog = Some("kyuubi_meta_db"),
            tableType = Some("TABLE"))
        } finally rs.close()
      } finally {
        statement.execute("drop table if exists kyuubi_meta_db.contract_probe")
        statement.execute("drop database if exists kyuubi_meta_db")
      }
    }
  }

  test("mysql - getColumns result has JDBC contract column names and column shape") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      statement.execute("create database if not exists kyuubi_meta_db")
      statement.execute("drop table if exists kyuubi_meta_db.contract_t")
      statement.execute(
        "create table kyuubi_meta_db.contract_t " +
          "(id BIGINT NOT NULL, name VARCHAR(64), age INT, created TIMESTAMP NULL) " +
          "ENGINE=InnoDB")
      try {
        val shapeRs = meta.getColumns("kyuubi_meta_db", null, "contract_t", null)
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
        assertColumnContract(
          shapeRs,
          "contract_t",
          Seq(
            ExpectedColumn("id", Some(java.sql.Types.BIGINT), Some(false)),
            ExpectedColumn("name", Some(java.sql.Types.VARCHAR), Some(true)),
            ExpectedColumn("age", Some(java.sql.Types.INTEGER), Some(true)),
            ExpectedColumn("created", nullable = Some(true))),
          // mysql-connector-j databaseTerm=CATALOG: database is in TABLE_CAT, schema null
          tableCatalog = Some("kyuubi_meta_db"),
          tableSchema = None)
      } finally {
        statement.execute("drop table if exists kyuubi_meta_db.contract_t")
        statement.execute("drop database if exists kyuubi_meta_db")
      }
    }
  }

  test("mysql - getPrimaryKeys returns single-column PK contract") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      statement.execute("create database if not exists kyuubi_meta_db")
      statement.execute("drop table if exists kyuubi_meta_db.pk_t")
      statement.execute(
        "create table kyuubi_meta_db.pk_t (id INT NOT NULL PRIMARY KEY, name VARCHAR(64)) " +
          "ENGINE=InnoDB")
      try {
        val rs = meta.getPrimaryKeys("kyuubi_meta_db", null, "pk_t")
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
        statement.execute("drop table if exists kyuubi_meta_db.pk_t")
        statement.execute("drop database if exists kyuubi_meta_db")
      }
    }
  }

  test("mysql - getCatalogs/getSchemas/getTableTypes/getTypeInfo/getFunctions result " +
    "schema contract") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val cats = meta.getCatalogs
      try assertJdbcSpecColumnNames(cats, Set("TABLE_CAT"))
      finally cats.close()
      val schemas = meta.getSchemas
      try assertJdbcSpecColumnNames(schemas, Set("TABLE_SCHEM", "TABLE_CATALOG"))
      finally schemas.close()
      val tt = meta.getTableTypes
      try assertJdbcSpecColumnNames(tt, Set("TABLE_TYPE"))
      finally tt.close()
      val types = meta.getTypeInfo
      try assertJdbcSpecColumnNames(types, Set("TYPE_NAME", "DATA_TYPE", "NULLABLE"))
      finally types.close()
      val fns = meta.getFunctions(null, null, "%")
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

  test("mysql - getCrossReference returns FK row contract") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      statement.execute("create database if not exists kyuubi_meta_db")
      statement.execute("drop table if exists kyuubi_meta_db.fk_child")
      statement.execute("drop table if exists kyuubi_meta_db.fk_parent")
      statement.execute(
        "create table kyuubi_meta_db.fk_parent (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
      statement.execute(
        "create table kyuubi_meta_db.fk_child (id INT NOT NULL PRIMARY KEY, parent_id INT, " +
          "CONSTRAINT fk_parent FOREIGN KEY (parent_id) " +
          "REFERENCES kyuubi_meta_db.fk_parent(id)) ENGINE=InnoDB")
      try {
        val rs = meta.getCrossReference(
          "kyuubi_meta_db",
          null,
          "fk_parent",
          "kyuubi_meta_db",
          null,
          "fk_child")
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
        // Filter to the exact (pk-table, fk-table, pk-col, fk-col) tuple rather than trusting
        // result ordering; drivers do not promise rows.head is the row we care about.
        val match_ = rows.filter { case (pkT, fkT, pkC, fkC, _) =>
          pkT.equalsIgnoreCase("fk_parent") && fkT.equalsIgnoreCase("fk_child") &&
          pkC.equalsIgnoreCase("id") && fkC.equalsIgnoreCase("parent_id")
        }
        assert(
          match_.size == 1,
          s"expected exactly one FK row matching parent.id <- child.parent_id, got rows=$rows")
        assert(match_.head._5 == 1)
      } finally {
        statement.execute("drop table if exists kyuubi_meta_db.fk_child")
        statement.execute("drop table if exists kyuubi_meta_db.fk_parent")
        statement.execute("drop database if exists kyuubi_meta_db")
      }
    }
  }
}
