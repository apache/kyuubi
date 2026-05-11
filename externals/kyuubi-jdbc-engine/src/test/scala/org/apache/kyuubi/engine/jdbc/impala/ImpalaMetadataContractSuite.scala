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
package org.apache.kyuubi.engine.jdbc.impala

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/**
 * Hive JDBC metadata contract. Regression guard for Impala HS2's TABLE_MD -> TABLE_SCHEM
 *  rewrite in ImpalaSchemaHelper.normalizeMetadataColumnLabel. PK / FK n/a in Impala.
 */
class ImpalaMetadataContractSuite extends WithImpalaEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = jdbcConnectionUrl

  test("impala - getTables result has JDBC contract column names") {
    withJdbcStatement() { statement =>
      statement.execute("create table if not exists default.contract_probe(id bigint)")
      try {
        val rs = statement.getConnection.getMetaData
          .getTables(null, "default", "contract_probe", null)
        try {
          assertJdbcSpecColumnNames(
            rs,
            Set("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"))
          assertTableRow(
            rs,
            "contract_probe",
            tableSchema = Some("default"),
            tableType = Some("TABLE"))
        } finally rs.close()
      } finally {
        statement.execute("drop table if exists default.contract_probe")
      }
    }
  }

  test("impala - getCatalogs/getSchemas/getTableTypes/getTypeInfo/getFunctions result " +
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

  test("impala - getColumns result has JDBC contract column names and column shape") {
    // Regression guard: backend reports TABLE_MD, must reach client as TABLE_SCHEM.
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      statement.execute("drop table if exists default.contract_t")
      statement.execute(
        "create table default.contract_t (id bigint, name string, age int)")
      try {
        val shapeRs = meta.getColumns(null, "default", "contract_t", null)
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
            ExpectedColumn("id"),
            ExpectedColumn("name"),
            ExpectedColumn("age")),
          tableSchema = Some("default"))
      } finally {
        statement.execute("drop table if exists default.contract_t")
      }
    }
  }
}
