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
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.DeltaCatalogRangerSparkExtensionSuite._
import org.apache.kyuubi.tags.DeltaTest
import org.apache.kyuubi.util.AssertionUtils._

/**
 * Tests for RangerSparkExtensionSuite on Delta Lake
 */
@DeltaTest
class DeltaCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
  override protected val sqlExtensions: String = "io.delta.sql.DeltaSparkSessionExtension"

  val namespace1 = deltaNamespace
  val table1 = "table1_delta"
  val table2 = "table2_delta"

  def createTableSql(namespace: String, table: String): String =
    s"""
       |CREATE TABLE IF NOT EXISTS $namespace.$table (
       |  id INT,
       |  name STRING,
       |  gender STRING,
       |  birthDate TIMESTAMP
       |)
       |USING DELTA
       |PARTITIONED BY (gender)
       |""".stripMargin

  override def withFixture(test: NoArgTest): Outcome = {
    test()
  }

  override def beforeAll(): Unit = {
    spark.conf.set(s"spark.sql.catalog.$sparkCatalog", deltaCatalogClassName)
    spark.conf.set(
      s"spark.sql.catalog.$sparkCatalog.warehouse",
      Utils.createTempDir("delta-hadoop").toString)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sessionState.catalog.reset()
    spark.sessionState.conf.clear()
  }

  test("create table") {
    withCleanTmpResources(Seq(
      (s"$namespace1.$table1", "table"),
      (s"$namespace1.$table2", "table"),
      (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      val createNonPartitionTableSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $namespace1.$table1 (
           |  id INT,
           |  name STRING,
           |  gender STRING,
           |  birthDate TIMESTAMP
           |) USING DELTA
           |""".stripMargin
      interceptContains[AccessControlException] {
        doAs(someone, sql(createNonPartitionTableSql))
      }(s"does not have [create] privilege on [$namespace1/$table1]")
      doAs(admin, createNonPartitionTableSql)

      val createPartitionTableSql = createTableSql(namespace1, table2)
      interceptContains[AccessControlException] {
        doAs(someone, sql(createPartitionTableSql))
      }(s"does not have [create] privilege on [$namespace1/$table2]")
      doAs(admin, createPartitionTableSql)
    }
  }

  test("create or replace table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      val createOrReplaceTableSql =
        s"""
           |CREATE OR REPLACE TABLE $namespace1.$table1 (
           |  id INT,
           |  name STRING,
           |  gender STRING,
           |  birthDate TIMESTAMP
           |) USING DELTA
           |""".stripMargin
      interceptContains[AccessControlException] {
        doAs(someone, sql(createOrReplaceTableSql))
      }(s"does not have [create] privilege on [$namespace1/$table1]")
      doAs(admin, createOrReplaceTableSql)
    }
  }

  test("alter table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(admin, sql(createTableSql(namespace1, table1)))

      // add columns
      interceptContains[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 ADD COLUMNS (age int)")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // change column
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(s"ALTER TABLE $namespace1.$table1" +
            s" CHANGE COLUMN gender gender STRING AFTER birthDate")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // replace columns
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(s"ALTER TABLE $namespace1.$table1" +
            s" REPLACE COLUMNS (id INT, name STRING)")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // rename column
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(s"ALTER TABLE $namespace1.$table1" +
            s" RENAME COLUMN birthDate TO dateOfBirth")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // drop column
      interceptContains[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 DROP COLUMN birthDate")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // set properties
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(s"ALTER TABLE $namespace1.$table1" +
            s" SET TBLPROPERTIES ('delta.appendOnly' = 'true')")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")
    }
  }

  test("delete from table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(admin, sql(createTableSql(namespace1, table1)))
      interceptContains[AccessControlException](
        doAs(someone, sql(s"DELETE FROM $namespace1.$table1 WHERE birthDate < '1955-01-01'")))(
        s"does not have [update] privilege on [$namespace1/$table1]")
    }
  }

  test("insert table") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq(
        (s"$namespace1.$table1", "table"),
        (s"$namespace1.$table2", "table"),
        (s"$namespace1", "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(admin, sql(createTableSql(namespace1, table1)))
        doAs(admin, sql(createTableSql(namespace1, table2)))

        // insert into
        interceptContains[AccessControlException](
          doAs(
            someone,
            sql(s"INSERT INTO $namespace1.$table1" +
              s" SELECT * FROM $namespace1.$table2")))(
          s"does not have [select] privilege on [$namespace1/$table2/id,$namespace1/$table2/name," +
            s"$namespace1/$table2/gender,$namespace1/$table2/birthDate]," +
            s" [update] privilege on [$namespace1/$table1]")

        // insert overwrite
        interceptContains[AccessControlException](
          doAs(
            someone,
            sql(s"INSERT INTO $namespace1.$table1" +
              s" SELECT * FROM $namespace1.$table2")))(
          s"does not have [select] privilege on [$namespace1/$table2/id,$namespace1/$table2/name," +
            s"$namespace1/$table2/gender,$namespace1/$table2/birthDate]," +
            s" [update] privilege on [$namespace1/$table1]")
      }
    }
  }

  test("update table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(admin, sql(createTableSql(namespace1, table1)))
      interceptContains[AccessControlException](
        doAs(
          someone,
          sql(s"UPDATE $namespace1.$table1" +
            s" SET gender = 'Female' WHERE gender = 'F'")))(
        s"does not have [update] privilege on [$namespace1/$table1]")
    }
  }
}

object DeltaCatalogRangerSparkExtensionSuite {
  val deltaCatalogClassName: String = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}
