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
package org.apache.kyuubi.engine.jdbc.phoenix

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/**
 * Hive JDBC metadata contract: column-name + key-field assertions per Thrift metadata op.
 *  Skipped due to phoenix-queryserver-client 6.0.0 driver gaps: getPrimaryKeys (returns 0
 *  rows), getTypeInfo (0 columns), getFunctions (unimplemented). FK n/a in Phoenix.
 */
class PhoenixMetadataContractSuite extends WithPhoenixEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = jdbcConnectionUrl

  test("phoenix - getTables result has JDBC contract column names") {
    withJdbcStatement() { statement =>
      statement.execute("drop table if exists db1.contract_probe")
      statement.execute("create table db1.contract_probe (id BIGINT NOT NULL PRIMARY KEY)")
      try {
        val rs = statement.getConnection.getMetaData
          .getTables(null, "DB1", "CONTRACT_PROBE", null)
        try {
          assertJdbcSpecColumnNames(
            rs,
            Set("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"))
          assertTableRow(
            rs,
            "CONTRACT_PROBE",
            tableSchema = Some("DB1"),
            tableType = Some("TABLE"))
        } finally rs.close()
      } finally {
        statement.execute("drop table if exists db1.contract_probe")
      }
    }
  }

  test("phoenix - getCatalogs/getSchemas/getTableTypes result schema contract") {
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
    }
  }

  test("phoenix - getColumns result has JDBC contract column names and column shape") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      statement.execute("drop table if exists db1.contract_t")
      statement.execute(
        "create table db1.contract_t " +
          "(id BIGINT NOT NULL PRIMARY KEY, name VARCHAR, age INTEGER)")
      try {
        val shapeRs = meta.getColumns(null, "DB1", "CONTRACT_T", null)
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
        // Phoenix Avatica reports IS_NULLABLE as true/false (not YES/NO); skip nullable here.
        assertColumnContract(
          shapeRs,
          "CONTRACT_T",
          Seq(
            ExpectedColumn("ID", Some(java.sql.Types.BIGINT)),
            ExpectedColumn("NAME", Some(java.sql.Types.VARCHAR)),
            ExpectedColumn("AGE", Some(java.sql.Types.INTEGER))),
          tableSchema = Some("DB1"))
      } finally {
        statement.execute("drop table if exists db1.contract_t")
      }
    }
  }
}
