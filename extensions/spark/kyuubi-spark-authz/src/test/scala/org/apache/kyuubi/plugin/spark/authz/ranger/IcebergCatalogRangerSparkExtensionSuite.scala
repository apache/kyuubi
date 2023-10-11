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

// scalastyle:off
import scala.util.Try

import org.scalatest.Outcome

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

  override def withFixture(test: NoArgTest): Outcome = {
    test()
  }

  override def beforeAll(): Unit = {
    spark.conf.set(
      s"spark.sql.catalog.$catalogV2",
      "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"spark.sql.catalog.$catalogV2.type", "hadoop")
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
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sessionState.catalog.reset()
    spark.sessionState.conf.clear()
  }

  test("[KYUUBI #3515] MERGE INTO") {
    val mergeIntoSql =
      s"""
         |MERGE INTO $catalogV2.$namespace1.$outputTable1 AS target
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
      s" on [$namespace1/$table1/id]"))

    withSingleCallEnabled {
      val e2 = intercept[AccessControlException](
        doAs(
          someone,
          sql(mergeIntoSql)))
      assert(e2.getMessage.contains(s"does not have" +
        s" [select] privilege" +
        s" on [$namespace1/$table1/id,$namespace1/table1/name,$namespace1/$table1/city]," +
        s" [update] privilege on [$namespace1/$outputTable1]"))
    }

    doAs(admin, sql(mergeIntoSql))
  }

  test("[KYUUBI #3515] UPDATE TABLE") {
    // UpdateTable
    val e1 = intercept[AccessControlException](
      doAs(
        someone,
        sql(s"UPDATE $catalogV2.$namespace1.$table1 SET city='Guangzhou' " +
          " WHERE id=1")))
    assert(e1.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$table1]"))

    doAs(
      admin,
      sql(s"UPDATE $catalogV2.$namespace1.$table1 SET city='Guangzhou' " +
        " WHERE id=1"))
  }

  test("[KYUUBI #3515] DELETE FROM TABLE") {
    // DeleteFromTable
    val e6 = intercept[AccessControlException](
      doAs(someone, sql(s"DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=2")))
    assert(e6.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$table1]"))

    doAs(admin, sql(s"DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=2"))
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

      interceptContains[AccessControlException](doAs(someone, sql(rewriteDataFiles1)))(
        s"does not have [alter] privilege on [$namespace1/$tableName]")
      interceptContains[AccessControlException](doAs(someone, sql(rewriteDataFiles2)))(
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
}
