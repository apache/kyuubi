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

import java.nio.file.Path

import org.scalatest.Outcome

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.ranger.DeltaCatalogRangerSparkExtensionSuite._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{isSparkV32OrGreater, isSparkV35OrGreater}
import org.apache.kyuubi.tags.DeltaTest
import org.apache.kyuubi.util.AssertionUtils._

/**
 * Tests for RangerSparkExtensionSuite on Delta Lake
 */
@DeltaTest
class DeltaCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
  override protected val supportPurge: Boolean = false
  override protected val sqlExtensions: String = "io.delta.sql.DeltaSparkSessionExtension"

  val namespace1 = deltaNamespace
  val table1 = "table1_delta"
  val table2 = "table2_delta"

  def propString(props: Map[String, String]): String =
    if (props.isEmpty) ""
    else {
      props
        .map { case (key, value) => s"'$key' = '$value'" }
        .mkString("TBLPROPERTIES (", ",", ")")
    }

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

  def createPathBasedTableSql(path: Path, props: Map[String, String] = Map.empty): String =
    s"""
       |CREATE TABLE IF NOT EXISTS delta.`$path` (
       |  id INT,
       |  name STRING,
       |  gender STRING,
       |  birthDate TIMESTAMP
       |)
       |USING DELTA
       |PARTITIONED BY (gender)
       |${propString(props)}
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
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createNonPartitionTableSql))
      }(s"does not have [create] privilege on [$namespace1/$table1]")
      doAs(admin, sql(createNonPartitionTableSql))

      val createPartitionTableSql = createTableSql(namespace1, table2)
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createPartitionTableSql))
      }(s"does not have [create] privilege on [$namespace1/$table2]")
      doAs(admin, sql(createPartitionTableSql))
    }
  }

  test("create or replace table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      val createOrReplaceTableSql =
        s"""
           |CREATE OR REPLACE TABLE $namespace1.$table1 (
           |  id INT,
           |  name STRING,
           |  gender STRING,
           |  birthDate TIMESTAMP
           |) USING DELTA
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createOrReplaceTableSql))
      }(s"does not have [create] privilege on [$namespace1/$table1]")
      doAs(admin, sql(createOrReplaceTableSql))
    }
  }

  test("alter table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(admin, sql(createTableSql(namespace1, table1)))

      // add columns
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 ADD COLUMNS (age int)")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // change column
      interceptEndsWith[AccessControlException](
        doAs(
          someone,
          sql(s"ALTER TABLE $namespace1.$table1" +
            s" CHANGE COLUMN gender gender STRING AFTER birthDate")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // replace columns
      interceptEndsWith[AccessControlException](
        doAs(
          someone,
          sql(s"ALTER TABLE $namespace1.$table1" +
            s" REPLACE COLUMNS (id INT, name STRING)")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // rename column
      interceptEndsWith[AccessControlException](
        doAs(
          someone,
          sql(s"ALTER TABLE $namespace1.$table1" +
            s" RENAME COLUMN birthDate TO dateOfBirth")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // drop column
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(s"ALTER TABLE $namespace1.$table1 DROP COLUMN birthDate")))(
        s"does not have [alter] privilege on [$namespace1/$table1]")

      // set properties
      interceptEndsWith[AccessControlException](
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
      val deleteFromTableSql = s"DELETE FROM $namespace1.$table1 WHERE birthDate < '1955-01-01'"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(deleteFromTableSql)))(
        s"does not have [update] privilege on [$namespace1/$table1]")
      doAs(admin, sql(deleteFromTableSql))
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
        val insertIntoSql = s"INSERT INTO $namespace1.$table1" +
          s" SELECT * FROM $namespace1.$table2"
        interceptEndsWith[AccessControlException](
          doAs(someone, sql(insertIntoSql)))(
          s"does not have [select] privilege on " +
            s"[$namespace1/$table2/birthDate,$namespace1/$table2/gender," +
            s"$namespace1/$table2/id,$namespace1/$table2/name]," +
            s" [update] privilege on [$namespace1/$table1]")
        doAs(admin, sql(insertIntoSql))

        // insert overwrite
        val insertOverwriteSql = s"INSERT OVERWRITE $namespace1.$table1" +
          s" SELECT * FROM $namespace1.$table2"
        interceptEndsWith[AccessControlException](
          doAs(someone, sql(insertOverwriteSql)))(
          s"does not have [select] privilege on " +
            s"[$namespace1/$table2/birthDate,$namespace1/$table2/gender," +
            s"$namespace1/$table2/id,$namespace1/$table2/name]," +
            s" [update] privilege on [$namespace1/$table1]")
        doAs(admin, sql(insertOverwriteSql))
      }
    }
  }

  test("update table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(admin, sql(createTableSql(namespace1, table1)))
      val updateTableSql = s"UPDATE $namespace1.$table1" +
        s" SET gender = 'Female' WHERE gender = 'F'"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(updateTableSql)))(
        s"does not have [update] privilege on [$namespace1/$table1]")
      doAs(admin, sql(updateTableSql))
    }
  }

  test("merge into table") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq(
        (s"$namespace1.$table1", "table"),
        (s"$namespace1.$table2", "table"),
        (s"$namespace1", "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(admin, sql(createTableSql(namespace1, table1)))
        doAs(admin, sql(createTableSql(namespace1, table2)))

        val mergeIntoSql =
          s"""
             |MERGE INTO $namespace1.$table1 AS target
             |USING $namespace1.$table2 AS source
             |ON target.id = source.id
             |WHEN MATCHED THEN
             |  UPDATE SET
             |    id = source.id,
             |    name = source.name,
             |    gender = source.gender,
             |    birthDate = source.birthDate
             |WHEN NOT MATCHED
             |  THEN INSERT (
             |    id,
             |    name,
             |    gender,
             |    birthDate
             |  )
             |  VALUES (
             |    source.id,
             |    source.name,
             |    source.gender,
             |    source.birthDate
             |  )
             |""".stripMargin
        interceptEndsWith[AccessControlException](
          doAs(someone, sql(mergeIntoSql)))(
          s"does not have [select] privilege on " +
            s"[$namespace1/$table2/birthDate,$namespace1/$table2/gender," +
            s"$namespace1/$table2/id,$namespace1/$table2/name]," +
            s" [update] privilege on [$namespace1/$table1]")
        doAs(admin, sql(mergeIntoSql))
      }
    }
  }

  test("optimize table") {
    assume(isSparkV32OrGreater, "optimize table is available in Delta Lake 1.2.0 and above")

    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(admin, sql(createTableSql(namespace1, table1)))
      val optimizeTableSql = s"OPTIMIZE $namespace1.$table1"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(optimizeTableSql)))(
        s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(optimizeTableSql))
    }
  }

  test("vacuum table") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (s"$namespace1", "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(admin, sql(createTableSql(namespace1, table1)))
      val vacuumTableSql = s"VACUUM $namespace1.$table1"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(vacuumTableSql)))(
        s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(vacuumTableSql))
    }
  }

  test("create path-based table") {
    withTempDir(path => {
      val createTableSql = createPathBasedTableSql(path)
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createTableSql))
      }(s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(createTableSql))
    })
  }

  test("create or replace path-based table") {
    withTempDir(path => {
      val createOrReplaceTableSql =
        s"""
           |CREATE OR REPLACE TABLE delta.`$path` (
           |  id INT,
           |  name STRING,
           |  gender STRING,
           |  birthDate TIMESTAMP
           |) USING DELTA
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createOrReplaceTableSql))
      }(s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(createOrReplaceTableSql))
    })
  }

  test("delete from path-based table") {
    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path)))
      val deleteFromTableSql = s"DELETE FROM delta.`$path` WHERE birthDate < '1955-01-01'"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(deleteFromTableSql))
      }(s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(deleteFromTableSql))
    })
  }

  test("update path-based table") {
    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path)))
      val updateTableSql = s"UPDATE delta.`$path` SET gender = 'Female' WHERE gender = 'F'"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(updateTableSql))
      }(s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(updateTableSql))
    })
  }

  test("insert path-based table") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq((s"$namespace1.$table2", "table"), (s"$namespace1", "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(admin, sql(createTableSql(namespace1, table2)))
        withTempDir(path => {
          doAs(admin, sql(createPathBasedTableSql(path)))
          // insert into
          val insertIntoSql = s"INSERT INTO delta.`$path` SELECT * FROM $namespace1.$table2"
          interceptEndsWith[AccessControlException](
            doAs(someone, sql(insertIntoSql)))(
            s"does not have [select] privilege on [$namespace1/$table2/birthDate," +
              s"$namespace1/$table2/gender,$namespace1/$table2/id," +
              s"$namespace1/$table2/name], [write] privilege on [[$path, $path/]]")
          doAs(admin, sql(insertIntoSql))

          // insert overwrite
          val insertOverwriteSql =
            s"INSERT OVERWRITE delta.`$path` SELECT * FROM $namespace1.$table2"
          interceptEndsWith[AccessControlException](
            doAs(someone, sql(insertOverwriteSql)))(
            s"does not have [select] privilege on [$namespace1/$table2/birthDate," +
              s"$namespace1/$table2/gender,$namespace1/$table2/id," +
              s"$namespace1/$table2/name], [write] privilege on [[$path, $path/]]")
          doAs(admin, sql(insertOverwriteSql))
        })
      }
    }
  }

  test("merge into path-based table") {
    withSingleCallEnabled {
      withCleanTmpResources(Seq(
        (s"$namespace1.$table2", "table"),
        (s"$namespace1", "database"))) {
        doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
        doAs(admin, sql(createTableSql(namespace1, table2)))
        withTempDir(path => {
          doAs(admin, sql(createPathBasedTableSql(path)))
          val mergeIntoSql =
            s"""
               |MERGE INTO delta.`$path` AS target
               |USING $namespace1.$table2 AS source
               |ON target.id = source.id
               |WHEN MATCHED THEN
               |  UPDATE SET
               |    id = source.id,
               |    name = source.name,
               |    gender = source.gender,
               |    birthDate = source.birthDate
               |WHEN NOT MATCHED
               |  THEN INSERT (
               |    id,
               |    name,
               |    gender,
               |    birthDate
               |  )
               |  VALUES (
               |    source.id,
               |    source.name,
               |    source.gender,
               |    source.birthDate
               |  )
               |""".stripMargin
          interceptEndsWith[AccessControlException](
            doAs(someone, sql(mergeIntoSql)))(
            s"does not have [select] privilege on [$namespace1/$table2/birthDate," +
              s"$namespace1/$table2/gender,$namespace1/$table2/id," +
              s"$namespace1/$table2/name], [write] privilege on [[$path, $path/]]")
          doAs(admin, sql(mergeIntoSql))
        })
      }
    }
  }

  test("optimize path-based table") {
    assume(isSparkV32OrGreater, "optimize table is available in Delta Lake 1.2.0 and above")

    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path)))
      val optimizeTableSql1 = s"OPTIMIZE delta.`$path`"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(optimizeTableSql1)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(optimizeTableSql1))

      val optimizeTableSql2 = s"OPTIMIZE '$path'"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(optimizeTableSql2)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(optimizeTableSql2))
    })
  }

  test("vacuum path-based table") {
    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path)))
      val vacuumTableSql1 = s"VACUUM delta.`$path`"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(vacuumTableSql1)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(vacuumTableSql1))

      val vacuumTableSql2 = s"VACUUM '$path'"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(vacuumTableSql2)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(vacuumTableSql2))
    })
  }

  test("alter path-based table set properties") {
    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path)))
      val setPropertiesSql = s"ALTER TABLE delta.`$path`" +
        s" SET TBLPROPERTIES ('delta.appendOnly' = 'true')"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(setPropertiesSql)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(setPropertiesSql))
    })
  }

  test("alter path-based table add columns") {
    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path)))
      val addColumnsSql = s"ALTER TABLE delta.`$path` ADD COLUMNS (age int)"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(addColumnsSql)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(addColumnsSql))
    })
  }

  test("alter path-based table change column") {
    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path)))
      val changeColumnSql = s"ALTER TABLE delta.`$path`" +
        s" CHANGE COLUMN gender gender STRING AFTER birthDate"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(changeColumnSql)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(changeColumnSql))
    })
  }

  test("alter path-based table drop column") {
    assume(
      isSparkV32OrGreater,
      "alter table drop column is available in Delta Lake 1.2.0 and above")

    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path, Map("delta.columnMapping.mode" -> "name"))))
      val dropColumnSql = s"ALTER TABLE delta.`$path` DROP COLUMN birthDate"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(dropColumnSql)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(dropColumnSql))
    })
  }

  test("alter path-based table rename column") {
    assume(
      isSparkV32OrGreater,
      "alter table rename column is available in Delta Lake 1.2.0 and above")

    withTempDir(path => {
      doAs(admin, sql(createPathBasedTableSql(path, Map("delta.columnMapping.mode" -> "name"))))
      val renameColumnSql = s"ALTER TABLE delta.`$path`" +
        s" RENAME COLUMN birthDate TO dateOfBirth"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(renameColumnSql)))(
        s"does not have [write] privilege on [[$path, $path/]]")
      doAs(admin, sql(renameColumnSql))
    })
  }

  test("alter path-based table replace columns") {
    withTempDir(path => {
      assume(
        isSparkV32OrGreater,
        "alter table replace columns is not available in Delta Lake 1.0.1")

      doAs(admin, sql(createPathBasedTableSql(path, Map("delta.columnMapping.mode" -> "name"))))
      val replaceColumnsSql = s"ALTER TABLE delta.`$path`" +
        s" REPLACE COLUMNS (id INT, name STRING, gender STRING)"
      interceptEndsWith[AccessControlException](
        doAs(someone, sql(replaceColumnsSql)))(
        s"does not have [write] privilege on [[$path, $path/]]")

      // There was a bug before Delta Lake 3.0, it will throw AnalysisException message
      // "Cannot drop column from a struct type with a single field:
      // StructType(StructField(birthDate,TimestampType,true))".
      // For details, see https://github.com/delta-io/delta/pull/1822
      if (isSparkV35OrGreater) {
        doAs(admin, sql(replaceColumnsSql))
      }
    })
  }
}

object DeltaCatalogRangerSparkExtensionSuite {
  val deltaCatalogClassName: String = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}
