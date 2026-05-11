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
package org.apache.kyuubi.engine.jdbc.starrocks

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/**
 * Hive JDBC metadata contract. getFunctions not asserted - mysql-connector-j SHOW predicate
 *  is rejected by StarRocks ("Only support equal predicate"). PK / FK n/a in StarRocks.
 */
class StarRocksMetadataContractSuite extends WithStarRocksEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = jdbcConnectionUrl

  test("starrocks - getTables result has JDBC contract column names") {
    withJdbcStatement() { statement =>
      statement.execute("create database if not exists kyuubi_meta_db")
      statement.execute("drop table if exists kyuubi_meta_db.contract_probe")
      statement.execute(
        "create table kyuubi_meta_db.contract_probe (id BIGINT) " +
          "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 3 " +
          "PROPERTIES ('replication_num' = '1')")
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

  test("starrocks - getCatalogs/getSchemas/getTableTypes/getTypeInfo result schema contract") {
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
    }
  }

  test("starrocks - getColumns result has JDBC contract column names and column shape") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      statement.execute("create database if not exists kyuubi_meta_db")
      statement.execute("drop table if exists kyuubi_meta_db.contract_t")
      statement.execute(
        "create table kyuubi_meta_db.contract_t " +
          "(id BIGINT, name VARCHAR(64), age INT) " +
          "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 3 " +
          "PROPERTIES ('replication_num' = '1')")
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
            ExpectedColumn("id", Some(java.sql.Types.BIGINT)),
            ExpectedColumn("name"),
            ExpectedColumn("age", Some(java.sql.Types.INTEGER))),
          // mysql-protocol: database in TABLE_CAT, schema null
          tableCatalog = Some("kyuubi_meta_db"),
          tableSchema = None)
      } finally {
        statement.execute("drop table if exists kyuubi_meta_db.contract_t")
        statement.execute("drop database if exists kyuubi_meta_db")
      }
    }
  }
}
