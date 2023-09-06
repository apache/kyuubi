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

import org.apache.spark.sql.connector.catalog.Identifier

import org.apache.kyuubi.spark.connector.hive.{HiveTable, HiveTableCatalog}
import org.apache.kyuubi.spark.connector.hive.command.DDLCommandTestUtils.V2_COMMAND_VERSION

class CreateTableSuite extends DDLCommandTestUtils {

  override protected def command: String = "CREATE TABLE"

  override protected def catalogVersion: String = "Hive V2"

  override protected def commandVersion: String = V2_COMMAND_VERSION

  test("Create datasource table") {
    val hiveCatalog = spark.sessionState.catalogManager
      .catalog(catalogName).asInstanceOf[HiveTableCatalog]
    val table = "hive.default.employee"
    Seq("parquet", "orc").foreach { provider =>
      withTable(table) {
        sql(
          s"""
             | CREATE TABLE IF NOT EXISTS
             | $table (id String, year String, month string)
             | USING $provider
             | PARTITIONED BY (year, month)
             |""".stripMargin).collect()
        val employee = Identifier.of(Array("default"), "employee")
        val loadTable = hiveCatalog.loadTable(employee)
        assert(loadTable.isInstanceOf[HiveTable])
        val catalogTable = loadTable.asInstanceOf[HiveTable].catalogTable
        assert(catalogTable.provider.isDefined)
        assert(catalogTable.provider.get.equalsIgnoreCase(provider))
      }
    }
  }

  test("Create hive table") {
    val hiveCatalog = spark.sessionState.catalogManager
      .catalog(catalogName).asInstanceOf[HiveTableCatalog]
    val table = "hive.default.employee"
    Seq("parquet", "orc").foreach { provider =>
      withTable(table) {
        sql(
          s"""
             | CREATE TABLE IF NOT EXISTS
             | $table (id String, year String, month string)
             | STORED AS $provider
             | PARTITIONED BY (year, month)
             |""".stripMargin).collect()
        val employee = Identifier.of(Array("default"), "employee")
        val loadTable = hiveCatalog.loadTable(employee)
        assert(loadTable.isInstanceOf[HiveTable])
        val catalogTable = loadTable.asInstanceOf[HiveTable].catalogTable
        assert(catalogTable.provider.isDefined)
        assert(catalogTable.provider.get.equalsIgnoreCase("hive"))
        assert(catalogTable.storage.serde.getOrElse("Unknown").contains(provider))
      }
    }
  }
}
