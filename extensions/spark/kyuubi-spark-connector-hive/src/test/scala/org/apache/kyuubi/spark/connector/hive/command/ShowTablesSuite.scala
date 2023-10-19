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

package org.apache.kyuubi.spark.connector.hive.command

import org.apache.spark.sql.Row

import org.apache.kyuubi.spark.connector.hive.command.DDLCommandTestUtils.{V1_COMMAND_VERSION, V2_COMMAND_VERSION}

trait ShowTablesSuiteBase extends DDLCommandTestUtils {
  override protected def command: String = "SHOW TABLES"

  private val defaultUsing = "USING parquet"

  protected def runShowTablesSql(sqlText: String, expected: Seq[Row]): Unit = {
    val df = spark.sql(sqlText)
    checkAnswer(df, expected)
  }

  test("show an existing table") {
    withNamespaceAndTable("ns", "table") { t =>
      sql(s"CREATE TABLE $t (name STRING, id INT) $defaultUsing")
      runShowTablesSql(s"SHOW TABLES IN $catalogName.ns", Seq(Row("ns", "table", false)))
    }
  }

  test("show tables with a pattern") {
    withNamespace(s"$catalogName.ns1", s"$catalogName.ns2") {
      sql(s"CREATE NAMESPACE $catalogName.ns1")
      sql(s"CREATE NAMESPACE $catalogName.ns2")
      withTable(
        s"$catalogName.ns1.table",
        s"$catalogName.ns1.table_name_1a",
        s"$catalogName.ns1.table_name_2b",
        s"$catalogName.ns2.table_name_2b") {
        sql(s"CREATE TABLE $catalogName.ns1.table (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalogName.ns1.table_name_1a (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalogName.ns1.table_name_2b (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalogName.ns2.table_name_2b (id bigint, data string) $defaultUsing")

        runShowTablesSql(
          s"SHOW TABLES FROM $catalogName.ns1",
          Seq(
            Row("ns1", "table", false),
            Row("ns1", "table_name_1a", false),
            Row("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalogName.ns1 LIKE '*name*'",
          Seq(
            Row("ns1", "table_name_1a", false),
            Row("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalogName.ns1 LIKE 'table_name_1*|table_name_2*'",
          Seq(
            Row("ns1", "table_name_1a", false),
            Row("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalogName.ns1 LIKE '*2b'",
          Seq(Row("ns1", "table_name_2b", false)))
      }
    }
  }

  test("change current catalog and namespace with USE statements") {
    withCurrentCatalogAndNamespace {
      withNamespaceAndTable("ns", "table") { t =>
        sql(s"CREATE TABLE $t (name STRING, id INT) $defaultUsing")

        sql(s"USE $catalogName")
        runShowTablesSql("SHOW TABLES", Seq())

        // Update the current namespace to match "ns.tbl".
        sql(s"USE $catalogName.ns")
        runShowTablesSql("SHOW TABLES", Seq(Row("ns", "table", false)))
      }
    }
  }
}

class ShowTablesV2Suite extends ShowTablesSuiteBase {

  override protected def catalogVersion: String = "Hive V2"

  override protected def commandVersion: String = V2_COMMAND_VERSION
}

class ShowTablesV1Suite extends ShowTablesSuiteBase {

  val SESSION_CATALOG_NAME: String = "spark_catalog"

  override protected val catalogName: String = SESSION_CATALOG_NAME

  override protected def catalogVersion: String = "V1"

  override protected def commandVersion: String = V1_COMMAND_VERSION
}
