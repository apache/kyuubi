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
  override protected val sqlExtensions: String =
    if (isSupportedVersion) "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
    else ""

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
      doAs(admin, sql(createTable))
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

  test("Rename Column Name") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val renameColumnSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |RENAME COLUMN name TO name1
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(renameColumnSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(renameColumnSql))
    }
  }

  test("Changing Column Position") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTableSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1
           |(id int, name string, a int, b int)
           |USING paimon
           |OPTIONS (
           |  'primary-key' = 'id'
           |)
           |""".stripMargin
      doAs(admin, sql(createTableSql))
      val changingColumnPositionToFirst =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |ALTER COLUMN a FIRST
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(changingColumnPositionToFirst))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(changingColumnPositionToFirst))

      val changingColumnPositionToAfter =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |ALTER COLUMN a AFTER name
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(changingColumnPositionToAfter))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(changingColumnPositionToAfter))
    }
  }

  test("REMOVING TBLEPROPERTIES") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTableWithPropertiesSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1
           |(id INT, name STRING)
           | USING paimon
           | TBLPROPERTIES (
           |  'write-buffer-size' = '256 MB'
           | )
           | OPTIONS (
           |  'primary-key' = 'id'
           | )
           |""".stripMargin
      doAs(admin, sql(createTableWithPropertiesSql))
      val removingTblpropertiesSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1 UNSET TBLPROPERTIES ('write-buffer-size')
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(removingTblpropertiesSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(removingTblpropertiesSql))
    }
  }

  test("Adding Column Position") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val addingColumnPositionFirst =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |ADD COLUMN a INT FIRST
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(addingColumnPositionFirst))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(addingColumnPositionFirst))

      val addingColumnPositionAfter =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |ADD COLUMN b INT AFTER a
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(addingColumnPositionAfter))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(addingColumnPositionAfter))
    }
  }

  test("Rename Table Name") {
    val table2 = "table2"
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"),
      (s"$catalogV2.$namespace1.$table2", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val renameTableNameSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1 RENAME TO $namespace1.$table2
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(renameTableNameSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(renameTableNameSql))
    }
  }

  test("INSERT INTO") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val insertSql =
        s"""
           |INSERT INTO $catalogV2.$namespace1.$table1 VALUES
           |(1, "a"), (2, "b");
           |""".stripMargin
      // Test user have select permission to insert
      doAs(table1OnlyUserForNs, sql(s"SELECT * FROM $catalogV2.$namespace1.$table1"))
      interceptEndsWith[AccessControlException] {
        doAs(table1OnlyUserForNs, sql(insertSql))
      }(s"does not have [update] privilege on [$namespace1/$table1]")

      // Test user have not any permission to insert
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(insertSql))
      }(s"does not have [update] privilege on [$namespace1/$table1]")

      doAs(admin, sql(insertSql))
    }
  }

  test("INSERT OVERWRITE") {
    val table2 = "table2"
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"),
      (s"$catalogV2.$namespace1.$table2", "table"))) {
      val createTable1 = createTableSql(namespace1, table1)
      val createTable2 = createTableSql(namespace1, table2)
      doAs(admin, sql(createTable1))
      doAs(admin, sql(createTable2))

      doAs(admin, sql(s"INSERT INTO $catalogV2.$namespace1.$table1 VALUES (1, 'a'), (2, 'b')"))

      val insertOverwriteSql =
        s"""
           |INSERT OVERWRITE $catalogV2.$namespace1.$table2
           |SELECT * FROM $catalogV2.$namespace1.$table1
           |""".stripMargin
      // Test user has select table1 permission to insert
      doAs(table1OnlyUserForNs, sql(s"SELECT * FROM $catalogV2.$namespace1.$table1"))
      interceptEndsWith[AccessControlException] {
        doAs(table1OnlyUserForNs, sql(insertOverwriteSql))
      }(s"does not have [update] privilege on [$namespace1/$table2]")

      // Test user has not any permission to insert
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(insertOverwriteSql))
      }(s"does not have [select] privilege on [$namespace1/$table1/id]")

      doAs(admin, sql(insertOverwriteSql))
    }
  }

  test("Changing Column Comment") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val changingColumnCommentSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |ALTER COLUMN name COMMENT "test comment"
           |""".stripMargin
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(changingColumnCommentSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(changingColumnCommentSql))
    }
  }

  test("Query") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val insertSql =
        s"""
           |INSERT INTO $catalogV2.$namespace1.$table1 VALUES
           |(1, "a"), (2, "b");
           |""".stripMargin
      doAs(admin, sql(insertSql))
      val querySql =
        s"""
           |SELECT id from $catalogV2.$namespace1.$table1
           |""".stripMargin

      doAs(table1OnlyUserForNs, sql(querySql).collect())
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(querySql).collect())
      }(s"does not have [select] privilege on [$namespace1/$table1/id]")
      doAs(admin, sql(querySql).collect())
    }
  }

  test("Batch Time Travel") {
    // Batch Time Travel requires Spark 3.3+
    if (isSparkV33OrGreater) {
      withCleanTmpResources(Seq(
        (s"$catalogV2.$namespace1.$table1", "table"))) {
        val createTable = createTableSql(namespace1, table1)
        doAs(admin, sql(createTable))
        val insertSql =
          s"""
             |INSERT INTO $catalogV2.$namespace1.$table1 VALUES
             |(1, "a"), (2, "b");
             |""".stripMargin
        doAs(admin, sql(insertSql))

        val querySnapshotVersionSql =
          s"""
             |SELECT id from $catalogV2.$namespace1.$table1 VERSION AS OF 1
             |""".stripMargin
        doAs(table1OnlyUserForNs, sql(querySnapshotVersionSql).collect())
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(querySnapshotVersionSql).collect())
        }(s"does not have [select] privilege on [$namespace1/$table1/id]")
        doAs(admin, sql(querySnapshotVersionSql).collect())

        val batchTimeTravelTimestamp =
          doAs(
            admin,
            sql(s"SELECT commit_time FROM $catalogV2.$namespace1.`$table1$$snapshots`" +
              s" ORDER BY commit_time ASC LIMIT 1").collect()(0).getTimestamp(0))

        val queryWithTimestamp =
          s"""
             |SELECT id FROM $catalogV2.$namespace1.$table1
             |TIMESTAMP AS OF '$batchTimeTravelTimestamp'
             |""".stripMargin
        doAs(table1OnlyUserForNs, sql(queryWithTimestamp).collect())
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(queryWithTimestamp).collect())
        }(s"does not have [select] privilege on [$namespace1/$table1/id]")
        doAs(admin, sql(queryWithTimestamp).collect())
      }
    }
  }

  test("Dropping Columns") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable = createTableSql(namespace1, table1)
      doAs(admin, sql(createTable))
      val droppingColumnsSql =
        s"""
           |ALTER TABLE $catalogV2.$namespace1.$table1
           |DROP COLUMNS (name)
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(droppingColumnsSql))
      }(s"does not have [alter] privilege on [$namespace1/$table1]")
      doAs(admin, sql(droppingColumnsSql))
    }
  }

  test("UPDATE & DELETE FROM") {
    if (isSupportedVersion) {
      withCleanTmpResources(Seq(
        (s"$catalogV2.$namespace1.$table1", "table"))) {
        val createTable = createTableSql(namespace1, table1)
        doAs(admin, sql(createTable))
        doAs(admin, sql(s"INSERT INTO $catalogV2.$namespace1.$table1 VALUES (1, 'a'), (2, 'b')"))

        val updateSql =
          s"""
             |UPDATE $catalogV2.$namespace1.$table1 SET name='c' where id=1
             |""".stripMargin
        val deleteSql =
          s"""
             |DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=1
             |""".stripMargin
        interceptEndsWith[AccessControlException] {
          doAs(table1OnlyUserForNs, sql(updateSql))
        }(s"does not have [update] privilege on [$namespace1/$table1]")
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(updateSql))
        }(s"does not have [update] privilege on [$namespace1/$table1]")
        doAs(admin, sql(updateSql))

        interceptEndsWith[AccessControlException] {
          doAs(table1OnlyUserForNs, sql(deleteSql))
        }(s"does not have [update] privilege on [$namespace1/$table1]")
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(deleteSql))
        }(s"does not have [update] privilege on [$namespace1/$table1]")
        doAs(admin, sql(deleteSql))
      }
    }
  }

  test("MERGE INTO") {
    if (isSupportedVersion) {
      val table2 = "table2"
      withCleanTmpResources(Seq(
        (s"$catalogV2.$namespace1.$table1", "table"),
        (s"$catalogV2.$namespace1.$table2", "table"))) {
        doAs(admin, sql(createTableSql(namespace1, table1)))
        doAs(admin, sql(createTableSql(namespace1, table2)))

        doAs(admin, sql(s"INSERT INTO $catalogV2.$namespace1.$table1 VALUES (1, 'a'), (2, 'b')"))
        doAs(admin, sql(s"INSERT INTO $catalogV2.$namespace1.$table1 VALUES (2, 'c'), (3, 'd')"))

        val mergeIntoSql =
          s"""
             |MERGE INTO $catalogV2.$namespace1.$table2
             |USING $catalogV2.$namespace1.$table1
             |ON
             |$catalogV2.$namespace1.$table2.id = $catalogV2.$namespace1.$table1.id
             |WHEN MATCHED THEN
             |  UPDATE SET name = $catalogV2.$namespace1.$table1.name
             |WHEN NOT MATCHED THEN
             |  INSERT *
             |""".stripMargin
        interceptEndsWith[AccessControlException] {
          doAs(table1OnlyUserForNs, sql(mergeIntoSql))
        }(s"does not have [update] privilege on [$namespace1/$table2]")
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(mergeIntoSql))
        }(s"does not have [select] privilege on [$namespace1/$table1]")
        doAs(admin, sql(mergeIntoSql))
      }
    }
  }

  test("Producers") {
    withCleanTmpResources(Seq(
      (s"$catalogV2.$namespace1.$table1", "table"))) {
      try {
        doAs(admin, sql(createTableSql(namespace1, table1)))
        doAs(admin, sql(s"INSERT INTO $catalogV2.$namespace1.$table1 VALUES (1, 'a'), (2, 'b')"))

        var currentCatalogName =
          doAs(admin, spark.sessionState.catalogManager.currentCatalog.name())

        doAs(admin, sql(s"use $catalogV2"))
        currentCatalogName = doAs(admin, spark.sessionState.catalogManager.currentCatalog.name())
        assert(currentCatalogName.equals(catalogV2))

        // Create Tag
        val createTagSql = s"Call sys.create_tag(table =>" +
          s"'$catalogV2.$namespace1.$table1', tag => 'test_tag', snapshot => 1)"
        interceptEndsWith[AccessControlException] {
          doAs(table1OnlyUserForNs, sql(createTagSql))
        }(s"does not have [alter] privilege on [$namespace1/$table1]")
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(createTagSql))
        }(s"does not have [alter] privilege on [$namespace1/$table1]")
        doAs(admin, sql(createTagSql))

        // Delete Tag
        val deleteTagSql = s"Call sys.delete_tag(table =>" +
          s"'$catalogV2.$namespace1.$table1', tag => 'test_tag')"
        interceptEndsWith[AccessControlException] {
          doAs(table1OnlyUserForNs, sql(deleteTagSql))
        }(s"does not have [alter] privilege on [$namespace1/$table1]")
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(deleteTagSql))
        }(s"does not have [alter] privilege on [$namespace1/$table1]")
        doAs(admin, sql(deleteTagSql))

        // Rollback
        doAs(admin, sql(s"INSERT INTO $catalogV2.$namespace1.$table1 VALUES (3, 'a'), (4, 'b')"))
        doAs(admin, sql(s"INSERT INTO $catalogV2.$namespace1.$table1 VALUES (5, 'a'), (6, 'b')"))
        val rollbackTagSql = s"Call sys.rollback(table =>" +
          s"'$catalogV2.$namespace1.$table1', version => '2')"
        interceptEndsWith[AccessControlException] {
          doAs(table1OnlyUserForNs, sql(rollbackTagSql))
        }(s"does not have [alter] privilege on [$namespace1/$table1]")
        interceptEndsWith[AccessControlException] {
          doAs(someone, sql(rollbackTagSql))
        }(s"does not have [alter] privilege on [$namespace1/$table1]")
        doAs(admin, sql(rollbackTagSql))

      } finally {
        doAs(admin, sql(s"use spark_catalog"))
      }
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
