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

import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.util.Try

import org.apache.spark.sql.Row
import org.scalatest.Outcome

// scalastyle:off
import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.tags.IcebergTest
import org.apache.kyuubi.util.AssertionUtils._

/**
 * Tests for RangerSparkExtensionSuite
 * on Iceberg catalog with DataSource V2 API.
 */
@IcebergTest
class IcebergCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
  override protected val sqlExtensions: String =
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  val catalogV2 = "local"
  val namespace1 = icebergNamespace
  val table1 = "table1"
  val outputTable1 = "outputTable1"
  val bobNamespace = "default_bob"
  val bobSelectTable = "table_select_bob_1"

  override def withFixture(test: NoArgTest): Outcome = {
    test()
  }

  override def beforeAll(): Unit = {
    spark.conf.set(
      s"spark.sql.catalog.$catalogV2",
      "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(
      s"spark.sql.catalog.$catalogV2.warehouse",
      Utils.createTempDir("iceberg-hadoop").toString)

    super.beforeAll()

    doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace1"))
    doAs(
      admin,
      sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1" +
        " (id int, name string, city string) USING iceberg"))

    doAs(
      admin,
      sql(s"INSERT INTO $catalogV2.$namespace1.$table1" +
        " (id , name , city ) VALUES (1, 'liangbowen','Guangzhou')"))
    doAs(
      admin,
      sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$outputTable1" +
        " (id int, name string, city string) USING iceberg"))

    doAs(
      admin,
      sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$bobNamespace.$bobSelectTable" +
        " (id int, name string, city string) USING iceberg"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sessionState.catalog.reset()
    spark.sessionState.conf.clear()
  }

  test("[KYUUBI #3515] MERGE INTO") {
    withSingleCallEnabled {
      val mergeIntoSql =
        s"""
           |MERGE INTO $catalogV2.$bobNamespace.$bobSelectTable AS target
           |USING $catalogV2.$namespace1.$table1  AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.name='delete') THEN DELETE
           |WHEN MATCHED AND (target.name='update') THEN UPDATE SET target.city = source.city
      """.stripMargin

      // MergeIntoTable:  Using a MERGE INTO Statement
      val e1 = intercept[AccessControlException](
        doAs(
          someone,
          sql(mergeIntoSql)))
      assert(e1.getMessage.contains(s"does not have [select] privilege" +
        s" on [$namespace1/$table1/city,$namespace1/$table1/id,$namespace1/$table1/name]"))

      withSingleCallEnabled {
        interceptEndsWith[AccessControlException](doAs(someone, sql(mergeIntoSql)))(
          if (isSparkV35OrGreater) {
            s"does not have [select] privilege on [$namespace1/table1/city" +
              s",$namespace1/$table1/id,$namespace1/$table1/name]"
          } else {
            "does not have " +
              s"[select] privilege on [$namespace1/$table1/city,$namespace1/$table1/id,$namespace1/$table1/name]," +
              s" [update] privilege on [$bobNamespace/$bobSelectTable]"
          })

        interceptEndsWith[AccessControlException] {
          doAs(bob, sql(mergeIntoSql))
        }(s"does not have [update] privilege on [$bobNamespace/$bobSelectTable]")
      }

      doAs(admin, sql(mergeIntoSql))
    }
  }

  test("[KYUUBI #3515] UPDATE TABLE") {
    withSingleCallEnabled {
      // UpdateTable
      interceptEndsWith[AccessControlException] {
        doAs(
          someone,
          sql(s"UPDATE $catalogV2.$namespace1.$table1 SET city='Guangzhou'  WHERE id=1"))
      }(if (isSparkV35OrGreater) {
        s"does not have [select] privilege on " +
          s"[$namespace1/$table1/_file,$namespace1/$table1/_pos," +
          s"$namespace1/$table1/id,$namespace1/$table1/name,$namespace1/$table1/city], " +
          s"[update] privilege on [$namespace1/$table1]"
      } else {
        s"does not have [update] privilege on [$namespace1/$table1]"
      })

      doAs(
        admin,
        sql(s"UPDATE $catalogV2.$namespace1.$table1 SET city='Guangzhou' " +
          " WHERE id=1"))
    }
  }

  test("[KYUUBI #3515] DELETE FROM TABLE") {
    withSingleCallEnabled {
      // DeleteFromTable
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(s"DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=2"))
      }(if (isSparkV34OrGreater) {
        s"does not have [select] privilege on " +
          s"[$namespace1/$table1/_file,$namespace1/$table1/_pos," +
          s"$namespace1/$table1/city,$namespace1/$table1/id,$namespace1/$table1/name], " +
          s"[update] privilege on [$namespace1/$table1]"
      } else {
        s"does not have [update] privilege on [$namespace1/$table1]"
      })

      interceptEndsWith[AccessControlException] {
        doAs(bob, sql(s"DELETE FROM $catalogV2.$bobNamespace.$bobSelectTable WHERE id=2"))
      }(s"does not have [update] privilege on [$bobNamespace/$bobSelectTable]")

      doAs(admin, sql(s"DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=2"))
    }
  }

  test("[KYUUBI #3666] Support {OWNER} variable for queries run on CatalogV2") {
    val table = "owner_variable"
    val select = s"SELECT key FROM $catalogV2.$namespace1.$table"

    withCleanTmpResources(Seq((s"$catalogV2.$namespace1.$table", "table"))) {
      doAs(
        defaultTableOwner,
        assert(Try {
          sql(s"CREATE TABLE $catalogV2.$namespace1.$table (key int, value int) USING iceberg")
        }.isSuccess))

      doAs(
        defaultTableOwner,
        assert(Try {
          sql(select).collect()
        }.isSuccess))

      doAs(
        createOnlyUser, {
          val e = intercept[AccessControlException](sql(select).collect())
          assert(e.getMessage === errorMessage("select", s"$namespace1/$table/key"))
        })
    }
  }

  test("KYUUBI #4047 MergeIntoIcebergTable with row filter") {
    assume(isSparkV32OrGreater)

    val outputTable2 = "outputTable2"
    withCleanTmpResources(Seq(
      (s"$catalogV2.default.src", "table"),
      (s"$catalogV2.default.outputTable2", "table"))) {
      doAs(
        admin,
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.default.src" +
          " (id int, name string, key string) USING iceberg"))
      doAs(
        admin,
        sql(s"INSERT INTO $catalogV2.default.src" +
          " (id , name , key ) VALUES " +
          "(1, 'liangbowen1','10')" +
          ", (2, 'liangbowen2','20')"))
      doAs(
        admin,
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$outputTable2" +
          " (id int, name string, key string) USING iceberg"))

      val mergeIntoSql =
        s"""
           |MERGE INTO $catalogV2.$namespace1.$outputTable2 AS target
           |USING $catalogV2.default.src  AS source
           |ON target.id = source.id
           |WHEN NOT MATCHED THEN INSERT (id, name, key) VALUES (source.id, source.name, source.key)
        """.stripMargin

      doAs(admin, sql(mergeIntoSql))
      doAs(
        admin, {
          val countOutputTable =
            sql(s"select count(1) from $catalogV2.$namespace1.$outputTable2").collect()
          val rowCount = countOutputTable(0).get(0)
          assert(rowCount === 2)
        })
      doAs(admin, sql(s"truncate table $catalogV2.$namespace1.$outputTable2"))

      // source table with row filter `key`<20
      doAs(bob, sql(mergeIntoSql))
      doAs(
        admin, {
          val countOutputTable =
            sql(s"select count(1) from $catalogV2.$namespace1.$outputTable2").collect()
          val rowCount = countOutputTable(0).get(0)
          assert(rowCount === 1)
        })
    }
  }

  test("[KYUUBI #4255] DESCRIBE TABLE") {
    val e1 = intercept[AccessControlException](
      doAs(someone, sql(s"DESCRIBE TABLE $catalogV2.$namespace1.$table1").explain()))
    assert(e1.getMessage.contains(s"does not have [select] privilege" +
      s" on [$namespace1/$table1]"))
  }

  test("CALL RewriteDataFilesProcedure") {
    val tableName = "table_select_call_command_table"
    val table = s"$catalogV2.$namespace1.$tableName"
    val initDataFilesCount = 2
    val rewriteDataFiles1 = s"CALL $catalogV2.system.rewrite_data_files " +
      s"(table => '$table', options => map('min-input-files','$initDataFilesCount'))"
    val rewriteDataFiles2 = s"CALL $catalogV2.system.rewrite_data_files " +
      s"(table => '$table', options => map('min-input-files','${initDataFilesCount + 1}'))"

    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin, {
          sql(s"CREATE TABLE IF NOT EXISTS $table  (id int, name string) USING iceberg")
          // insert 2 data files
          (0 until initDataFilesCount)
            .foreach(i => sql(s"INSERT INTO $table VALUES ($i, 'user_$i')"))
        })

      interceptEndsWith[AccessControlException](doAs(someone, sql(rewriteDataFiles1)))(
        s"does not have [alter] privilege on [$namespace1/$tableName]")
      interceptEndsWith[AccessControlException](doAs(someone, sql(rewriteDataFiles2)))(
        s"does not have [alter] privilege on [$namespace1/$tableName]")

      /**
       * Case 1: Number of input data files equals or greater than minimum expected.
       * Two logical plans triggered
       * when ( input-files(2) >= min-input-files(2) ):
       *
       * == Physical Plan 1 ==
       * Call (1)
       *
       * == Physical Plan 2 ==
       * AppendData (3)
       * +- * ColumnarToRow (2)
       * +- BatchScan local.iceberg_ns.call_command_table (1)
       */
      doAs(
        admin, {
          val result1 = sql(rewriteDataFiles1).collect()
          // rewritten results into 2 data files
          assert(result1(0)(0) === initDataFilesCount)
        })

      /**
       * Case 2: Number of input data files less than minimum expected.
       * Only one logical plan triggered
       * when ( input-files(2) < min-input-files(3) )
       *
       * == Physical Plan ==
       *  Call (1)
       */
      doAs(
        admin, {
          val result2 = sql(rewriteDataFiles2).collect()
          assert(result2(0)(0) === 0)
        })
    }
  }

  private def prepareExampleIcebergTable(table: String, initSnapshots: Int): Unit = {
    doAs(admin, sql(s"CREATE TABLE IF NOT EXISTS $table (id int, name string) USING iceberg"))
    (0 until initSnapshots).foreach(i =>
      doAs(admin, sql(s"INSERT INTO $table VALUES ($i, 'user_$i')")))
  }

  private def getFirstSnapshot(table: String): Row = {
    val existedSnapshots =
      sql(s"SELECT * FROM $table.snapshots ORDER BY committed_at ASC LIMIT 1").collect()
    existedSnapshots(0)
  }

  test("CALL rollback_to_snapshot") {
    val tableName = "table_rollback_to_snapshot"
    val table = s"$catalogV2.$namespace1.$tableName"
    withCleanTmpResources(Seq((table, "table"))) {
      prepareExampleIcebergTable(table, 2)
      val targetSnapshotId = getFirstSnapshot(table).getAs[Long]("snapshot_id")
      val callRollbackToSnapshot =
        s"CALL $catalogV2.system.rollback_to_snapshot (table => '$table', snapshot_id => $targetSnapshotId)"

      interceptEndsWith[AccessControlException](doAs(someone, sql(callRollbackToSnapshot)))(
        s"does not have [alter] privilege on [$namespace1/$tableName]")
      doAs(admin, sql(callRollbackToSnapshot))
    }
  }

  test("CALL rollback_to_timestamp") {
    val tableName = "table_rollback_to_timestamp"
    val table = s"$catalogV2.$namespace1.$tableName"
    withCleanTmpResources(Seq((table, "table"))) {
      prepareExampleIcebergTable(table, 2)
      val callRollbackToTimestamp = {
        val targetSnapshotCommittedAt = getFirstSnapshot(table).getAs[Timestamp]("committed_at")
        val targetTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          .format(targetSnapshotCommittedAt.getTime + 1)
        s"CALL $catalogV2.system.rollback_to_timestamp (table => '$table', timestamp => TIMESTAMP '$targetTimestamp')"
      }

      interceptEndsWith[AccessControlException](doAs(someone, sql(callRollbackToTimestamp)))(
        s"does not have [alter] privilege on [$namespace1/$tableName]")
      doAs(admin, sql(callRollbackToTimestamp))
    }
  }

  test("CALL set_current_snapshot") {
    val tableName = "table_set_current_snapshot"
    val table = s"$catalogV2.$namespace1.$tableName"
    withCleanTmpResources(Seq((table, "table"))) {
      prepareExampleIcebergTable(table, 2)
      val targetSnapshotId = getFirstSnapshot(table).getAs[Long]("snapshot_id")
      val callSetCurrentSnapshot =
        s"CALL $catalogV2.system.set_current_snapshot (table => '$table', snapshot_id => $targetSnapshotId)"

      interceptEndsWith[AccessControlException](doAs(someone, sql(callSetCurrentSnapshot)))(
        s"does not have [alter] privilege on [$namespace1/$tableName]")
      doAs(admin, sql(callSetCurrentSnapshot))
    }
  }

  test("ALTER TABLE ADD PARTITION FIELD for Iceberg") {
    val table = s"$catalogV2.$namespace1.partitioned_table"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin,
        sql(
          s"CREATE TABLE $table (id int, name string, city string) USING iceberg PARTITIONED BY (city)"))
      val addPartitionSql = s"ALTER TABLE $table ADD PARTITION FIELD id"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(addPartitionSql))
      }(s"does not have [alter] privilege on [$namespace1/partitioned_table]")
      doAs(admin, sql(addPartitionSql))
    }
  }

  test("ALTER TABLE DROP PARTITION FIELD for Iceberg") {
    val table = s"$catalogV2.$namespace1.partitioned_table"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin,
        sql(
          s"CREATE TABLE $table (id int, name string, city string) USING iceberg PARTITIONED BY (id, city)"))
      val addPartitionSql = s"ALTER TABLE $table DROP PARTITION FIELD id"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(addPartitionSql))
      }(s"does not have [alter] privilege on [$namespace1/partitioned_table]")
      doAs(admin, sql(addPartitionSql))
    }
  }

  test("ALTER TABLE REPLACE PARTITION FIELD for Iceberg") {
    val table = s"$catalogV2.$namespace1.partitioned_table"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin,
        sql(
          s"CREATE TABLE $table (id int, name string, city string) USING iceberg PARTITIONED BY (city)"))
      val addPartitionSql = s"ALTER TABLE $table REPLACE PARTITION FIELD city WITH id"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(addPartitionSql))
      }(s"does not have [alter] privilege on [$namespace1/partitioned_table]")
      doAs(admin, sql(addPartitionSql))
    }
  }

  test("ALTER TABLE WRITE ORDER BY for Iceberg") {
    val table = s"$catalogV2.$namespace1.partitioned_table"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin,
        sql(
          s"CREATE TABLE $table (id int, name string, city string) USING iceberg"))
      val writeOrderBySql = s"ALTER TABLE $table WRITE ORDERED BY id"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(writeOrderBySql))
      }(s"does not have [alter] privilege on [$namespace1/partitioned_table]")
      doAs(admin, sql(writeOrderBySql))
    }
  }

  test("ALTER TABLE WRITE DISTRIBUTED BY PARTITION for Iceberg") {
    val table = s"$catalogV2.$namespace1.partitioned_table"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin,
        sql(
          s"CREATE TABLE $table (id int, name string, city string) USING iceberg PARTITIONED BY (city)"))
      val writeDistributedSql = s"ALTER TABLE $table WRITE DISTRIBUTED BY PARTITION"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(writeDistributedSql))
      }(s"does not have [alter] privilege on [$namespace1/partitioned_table]")
      doAs(admin, sql(writeDistributedSql))
    }
  }

  test("ALTER TABLE SET IDENTIFIER FIELD for Iceberg") {
    val table = s"$catalogV2.$namespace1.partitioned_table"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin,
        sql(
          s"CREATE TABLE $table (id int NOT NULL, name string, city string) USING iceberg"))
      val setIdentifierSql = s"ALTER TABLE $table SET IDENTIFIER FIELDS id"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(setIdentifierSql))
      }(s"does not have [alter] privilege on [$namespace1/partitioned_table]")
      doAs(admin, sql(setIdentifierSql))
    }
  }

  test("ALTER TABLE DROP IDENTIFIER FIELD for Iceberg") {
    val table = s"$catalogV2.$namespace1.partitioned_table"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs(
        admin,
        sql(
          s"CREATE TABLE $table (id int NOT NULL, name string, city string) USING iceberg"))
      doAs(admin, sql(s"ALTER TABLE $table SET IDENTIFIER FIELDS id"))
      val dropIdentifierSql = s"ALTER TABLE $table DROP IDENTIFIER FIELDS id"
      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(dropIdentifierSql))
      }(s"does not have [alter] privilege on [$namespace1/partitioned_table]")
      doAs(admin, sql(dropIdentifierSql))
    }
  }

  test("RENAME TABLE for Iceberg") {
    withSingleCallEnabled {
      doAs(admin, sql(s"alter table $catalogV2.$namespace1.$table1 rename to $catalogV2.$namespace1.table_new_1"))
    }
  }

}
