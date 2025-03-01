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
package org.apache.kyuubi.plugin.spark.authz.ranger

import org.scalatest.Outcome

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.tags.PaimonTest
import org.apache.kyuubi.util.AssertionUtils._

/**
 * Tests for RangerSparkExtensionSuite on Paimon
 */
@PaimonTest
class PaimonCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
  private def isSupportedVersion = isScalaV212

  val catalogV2 = "paimon_catalog"
  val namespace1 = "paimon_ns"
  val table1 = "table1"

  override def withFixture(test: NoArgTest): Outcome = {
    assume(isSupportedVersion)
    test()
  }

  override def beforeAll(): Unit = {
    if (isSupportedVersion) {
      spark.conf.set(s"spark.sql.catalog.$catalogV2", "org.apache.paimon.spark.SparkCatalog")
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2.warehouse",
        Utils.createTempDir(catalogV2).toString)
      super.beforeAll()
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace1"))
    }
  }

  override def afterAll(): Unit = {
    if (isSupportedVersion) {
      doAs(admin, sql(s"DROP DATABASE IF EXISTS $catalogV2.$namespace1"))

      super.afterAll()
      spark.sessionState.catalog.reset()
      spark.sessionState.conf.clear()
    }
  }

  test("CreateTable") {
    withCleanTmpResources(Seq((s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable =
        s"""
           |CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1
           |(id int, name string, city string)
           |USING paimon
           |OPTIONS (
           | primaryKey = 'id'
           |)
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createTable))
      }(s"does not have [create] privilege on [$namespace1/$table1]")
      doAs(admin, createTable)
    }
  }

  test("[KYUUBI #6541] INSERT/SELECT TABLE") {
    val tName = "t_paimon"
    withCleanTmpResources(Seq((s"$catalogV2.$namespace1.$tName", "table"))) {
      doAs(bob, sql(createTableSql(namespace1, tName)))

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(s"INSERT INTO $catalogV2.$namespace1.$tName VALUES (1, 'name_1')"))
      }(s"does not have [update] privilege on [$namespace1/$tName]")
      doAs(bob, sql(s"INSERT INTO $catalogV2.$namespace1.$tName VALUES (1, 'name_1')"))
      doAs(bob, sql(s"INSERT INTO $catalogV2.$namespace1.$tName VALUES (1, 'name_2')"))

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(s"SELECT id FROM $catalogV2.$namespace1.$tName").show())
      }(s"does not have [select] privilege on [$namespace1/$tName/id]")
      doAs(bob, sql(s"SELECT name FROM $catalogV2.$namespace1.$tName").show())
    }
  }

  test("CTAS") {
    val table2 = "table2"
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"),
      (s"$catalogV2.$namespace1.$table2", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val createTableAsSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table2
           |USING paimon
           |AS
           |SELECT * FROM $catalogV2.$namespace1.$table1
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createTableAsSql))
      }(s"does not have [select] privilege on [$namespace1/$table1/id]")
      doAs(admin, sql(createTableAsSql))
    }
  }

  test("ALTER TBLPROPERTIES") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val addTblPropertiesSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1 SET TBLPROPERTIES(
           |  'write-buffer-size' = '256 MB'
           |)
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(addTblPropertiesSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(addTblPropertiesSql))
      val changeTblPropertiesSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1 SET TBLPROPERTIES(
           |  'write-buffer-size' = '128 MB'
           |)
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(changeTblPropertiesSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(changeTblPropertiesSql))
    }
  }

  test("CREATE PARTITIONED Table") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createPartitionTableSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1
           |(id INT, name STRING, dt STRING, hh STRING)
           | USING paimon
           | PARTITIONED BY (dt, hh)
           | OPTIONS (
           |  'primary-key' = 'id'
           | )
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createPartitionTableSql))
      }(s"does not have [create] privilege on [$namespace1/$table1]")
      doAs(admin, sql(createPartitionTableSql))
    }
  }

  test("Add New Column") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val alterTableSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |ADD COLUMNS (
           |  city STRING
           |)
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(alterTableSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(alterTableSql))
    }
  }

  def createTableSql(namespace: String, table: String): String =
    s"""
       |CREATE TABLE IF NOT EXISTS $catalogV2.$namespace.$table
       |(id int, name string)
       |USING paimon
       |OPTIONS (
       | 'primary-key' = 'id'
       |)
       |""".stripMargin

}
