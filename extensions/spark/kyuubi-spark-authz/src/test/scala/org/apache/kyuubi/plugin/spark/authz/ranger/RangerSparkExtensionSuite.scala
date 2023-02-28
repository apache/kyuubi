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

import org.apache.spark.sql.{Row, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.scalatest.BeforeAndAfterAll
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider
import org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization.KYUUBI_AUTHZ_TAG
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.getFieldVal

abstract class RangerSparkExtensionSuite extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll {
  // scalastyle:on
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  protected def errorMessage(
      privilege: String,
      resource: String = "default/src"): String = {
    s"does not have [$privilege] privilege on [$resource]"
  }

  /**
   * Drops temporary view `viewNames` after calling `f`.
   */
  protected def withTempView(viewNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      viewNames.foreach { viewName =>
        try spark.catalog.dropTempView(viewName)
        catch {
          // If the test failed part way, we don't want to mask the failure by failing to remove
          // temp views that never got created.
          case _: NoSuchTableException =>
        }
      }
    }
  }

  /**
   * Drops global temporary view `viewNames` after calling `f`.
   */
  protected def withGlobalTempView(viewNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      viewNames.foreach { viewName =>
        try spark.catalog.dropGlobalTempView(viewName)
        catch {
          // If the test failed part way, we don't want to mask the failure by failing to remove
          // global temp views that never got created.
          case _: NoSuchTableException =>
        }
      }
    }
  }

  test("[KYUUBI #3226] RuleAuthorization: Should check privileges once only.") {
    val logicalPlan = doAs("admin")(sql("SHOW TABLES").queryExecution.logical)
    val rule = new RuleAuthorization(spark)

    (1 until 10).foreach { i =>
      if (i == 1) {
        assert(logicalPlan.getTagValue(KYUUBI_AUTHZ_TAG).isEmpty)
      } else {
        assert(logicalPlan.getTagValue(KYUUBI_AUTHZ_TAG).getOrElse(false))
      }
      rule.apply(logicalPlan)
    }

    assert(logicalPlan.getTagValue(KYUUBI_AUTHZ_TAG).getOrElse(false))
  }

  test("[KYUUBI #3226]: Another session should also check even if the plan is cached.") {
    val testTable = "mytable"
    val create =
      s"""
         | CREATE TABLE IF NOT EXISTS $testTable
         | (id string)
         | using parquet
         |""".stripMargin
    val select = s"SELECT * FROM $testTable"

    withCleanTmpResources(Seq((testTable, "table"))) {
      // create tmp table
      doAs("admin") {
        sql(create)

        // session1: first query, should auth once.[LogicalRelation]
        val df = sql(select)
        val plan1 = df.queryExecution.optimizedPlan
        assert(plan1.getTagValue(KYUUBI_AUTHZ_TAG).getOrElse(false))

        // cache
        df.cache()

        // session1: second query, should auth once.[InMemoryRelation]
        // (don't need to check in again, but it's okay to check in once)
        val plan2 = sql(select).queryExecution.optimizedPlan
        assert(plan1 != plan2 && plan2.getTagValue(KYUUBI_AUTHZ_TAG).getOrElse(false))

        // session2: should auth once.
        val otherSessionDf = spark.newSession().sql(select)

        // KYUUBI_AUTH_TAG is None
        assert(otherSessionDf.queryExecution.logical.getTagValue(KYUUBI_AUTHZ_TAG).isEmpty)
        val plan3 = otherSessionDf.queryExecution.optimizedPlan
        // make sure it use cache.
        assert(plan3.isInstanceOf[InMemoryRelation])
        // auth once only.
        assert(plan3.getTagValue(KYUUBI_AUTHZ_TAG).getOrElse(false))
      }
    }
  }

  test("auth: databases") {
    val testDb = "mydb"
    val create = s"CREATE DATABASE IF NOT EXISTS $testDb"
    val alter = s"ALTER DATABASE $testDb SET DBPROPERTIES (abc = '123')"
    val drop = s"DROP DATABASE IF EXISTS $testDb"

    runSqlAsWithAccessException()(create, contains = errorMessage("create", "mydb"))
    withCleanTmpResources(Seq((testDb, "database"))) {
      runSqlAs("admin")(create)
      runSqlAs("admin")(alter)
      runSqlAsWithAccessException()(alter, contains = errorMessage("alter", "mydb"))
      runSqlAsWithAccessException()(drop, contains = errorMessage("drop", "mydb"))
      runSqlAs("kent")("SHOW DATABASES")
    }
  }

  test("auth: tables") {
    val db = "default"
    val table = "src"
    val col = "key"

    val create0 = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"
    val alter0 = s"ALTER TABLE $db.$table SET TBLPROPERTIES(key='ak')"
    val drop0 = s"DROP TABLE IF EXISTS $db.$table"
    val select = s"SELECT * FROM $db.$table"
    runSqlAsWithAccessException()(create0, contains = errorMessage("create"))

    withCleanTmpResources(Seq((s"$db.$table", "table"))) {
      runSqlAs("bob")(create0)
      runSqlAs("bob")(alter0)

      runSqlAsWithAccessException()(drop0, contains = errorMessage("drop"))
      runSqlAs("bob")(alter0)
      runSqlAs("bob")(select, isCollect = true)
      runSqlAs("kent")(s"SELECT key FROM $db.$table", isCollect = true)

      Seq(
        select,
        s"SELECT value FROM $db.$table",
        s"SELECT value as key FROM $db.$table",
        s"SELECT max(value) FROM $db.$table",
        s"SELECT coalesce(max(value), 1) FROM $db.$table",
        s"SELECT key FROM $db.$table WHERE value in (SELECT value as key FROM $db.$table)")
        .foreach { q =>
          runSqlAsWithAccessException("kent")(
            q,
            isCollect = true,
            contains = errorMessage("select", "default/src/value"))
        }
    }
  }

  test("auth: functions") {
    val db = "default"
    val func = "func"
    val create0 = s"CREATE FUNCTION IF NOT EXISTS $db.$func AS 'abc.mnl.xyz'"
    runSqlAsWithAccessException("kent")(create0, contains = errorMessage("create", "default/func"))
    runSqlAs("admin")(create0)
  }

  test("row level filter") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"

    withCleanTmpResources(Seq((s"$db.${table}2", "table"), (s"$db.$table", "table"))) {
      runSqlAs("admin")(create)
      runSqlAs("admin")(s"INSERT INTO $db.$table SELECT 1, 1")
      runSqlAs("admin")(s"INSERT INTO $db.$table SELECT 20, 2")
      runSqlAs("admin")(s"INSERT INTO $db.$table SELECT 30, 3")

      doAs("kent")(
        assert(sql(s"SELECT key FROM $db.$table order by key").collect() ===
          Seq(Row(1), Row(20), Row(30))))

      Seq(
        s"SELECT value FROM $db.$table",
        s"SELECT value as key FROM $db.$table",
        s"SELECT max(value) FROM $db.$table",
        s"SELECT coalesce(max(value), 1) FROM $db.$table",
        s"SELECT value FROM $db.$table WHERE value in (SELECT value as key FROM $db.$table)")
        .foreach { q =>
          doAs("bob") {
            withClue(q) {
              assert(sql(q).collect() === Seq(Row(1)))
            }
          }
        }
      doAs("bob") {
        sql(s"CREATE TABLE $db.src2 using $format AS SELECT value FROM $db.$table")
        assert(sql(s"SELECT value FROM $db.${table}2").collect() === Seq(Row(1)))
      }
    }
  }

  test("[KYUUBI #3581]: row level filter on permanent view") {
    assume(isSparkV31OrGreater)

    val db = "default"
    val table = "src"
    val permView = "perm_view"
    val col = "key"
    val create = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"
    val createView =
      s"CREATE OR REPLACE VIEW $db.$permView" +
        s" AS SELECT * FROM $db.$table"

    withCleanTmpResources(Seq(
      (s"$db.$table", "table"),
      (s"$db.$permView", "view"))) {
      runSqlAs("admin")(create)
      runSqlAs("admin")(createView)
      runSqlAs("admin")(s"INSERT INTO $db.$table SELECT 1, 1")
      runSqlAs("admin")(s"INSERT INTO $db.$table SELECT 20, 2")
      runSqlAs("admin")(s"INSERT INTO $db.$table SELECT 30, 3")

      Seq(
        s"SELECT value FROM $db.$permView",
        s"SELECT value as key FROM $db.$permView",
        s"SELECT max(value) FROM $db.$permView",
        s"SELECT coalesce(max(value), 1) FROM $db.$permView",
        s"SELECT value FROM $db.$permView WHERE value in (SELECT value as key FROM $db.$permView)")
        .foreach { q =>
          doAs("perm_view_user") {
            withClue(q) {
              assert(sql(q).collect() === Seq(Row(1)))
            }
          }
        }
    }
  }

  test("show tables") {
    val db = "default2"
    val table = "src"
    withCleanTmpResources(
      Seq(
        (s"$db.$table", "table"),
        (s"$db.${table}for_show", "table"),
        (s"$db", "database"))) {
      runSqlAs("admin")(s"CREATE DATABASE IF NOT EXISTS $db")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.$table (key int) USING $format")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.${table}for_show (key int) USING $format")

      runSqlAs("admin")(s"show tables from $db", collectSize = 2)
      runSqlAs("bob")(s"show tables from $db", collectSize = 0)
      runSqlAs("i_am_invisible")(s"show tables from $db", collectSize = 0)
    }
  }

  test("show databases") {
    val db = "default2"

    withCleanTmpResources(Seq((db, "database"))) {
      runSqlAs("admin")(s"CREATE DATABASE IF NOT EXISTS $db")
      runSqlAs("admin")(s"SHOW DATABASES", collectSize = 2)
      doAs("admin")(assert(sql(s"SHOW DATABASES").collectAsList().get(0).getString(0) == "default"))
      doAs("admin")(assert(sql(s"SHOW DATABASES").collectAsList().get(1).getString(0) == s"$db"))

      runSqlAs("bob")("SHOW DATABASES", collectSize = 1)
      doAs("bob")(assert(sql(s"SHOW DATABASES").collectAsList().get(0).getString(0) == "default"))
    }
  }

  test("show functions") {
    val default = "default"
    val db3 = "default3"
    val function1 = "function1"

    withCleanTmpResources(Seq(
      (s"$default.$function1", "function"),
      (s"$db3.$function1", "function"),
      (db3, "database"))) {
      runSqlAs("admin")(s"CREATE FUNCTION $function1 AS 'Function1'")
      runSqlAs("admin")(s"show user functions $default.$function1", collectSize = 1)
      runSqlAs("bob")(s"show user functions $default.$function1", collectSize = 0)

      runSqlAs("admin")(s"CREATE DATABASE IF NOT EXISTS $db3")
      runSqlAs("admin")(s"CREATE FUNCTION $db3.$function1 AS 'Function1'")

      runSqlAs("admin")(s"show user functions $db3.$function1", collectSize = 1)
      runSqlAs("bob")(s"show user functions $db3.$function1", collectSize = 0)

      val showSysFunctions = s"show system functions"
      doAs("admin")(assert(sql(showSysFunctions).collect().length > 0))
      doAs("bob")(assert(sql(showSysFunctions).collect().length > 0))

      val adminSystemFunctionCount = doAs("admin")(sql(showSysFunctions).collect().length)
      val bobSystemFunctionCount = doAs("bob")(sql(showSysFunctions).collect().length)
      assert(adminSystemFunctionCount == bobSystemFunctionCount)
    }
  }

  test("show columns") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"

    withCleanTmpResources(Seq((s"$db.$table", "table"))) {
      runSqlAs("admin")(create)

      runSqlAs("admin")(s"SHOW COLUMNS IN $table", countSize = 2)
      runSqlAs("admin")(s"SHOW COLUMNS IN $db.$table", countSize = 2)
      runSqlAs("admin")(s"SHOW COLUMNS IN $table IN $db", countSize = 2)

      runSqlAs("kent")(s"SHOW COLUMNS IN $table", countSize = 1)
      runSqlAs("kent")(s"SHOW COLUMNS IN $db.$table", countSize = 1)
      runSqlAs("kent")(s"SHOW COLUMNS IN $table IN $db", countSize = 1)
    }
  }

  test("show table extended") {
    val db = "default_bob"
    val table = "table"

    withCleanTmpResources(Seq(
      (s"$db.${table}_use1", "table"),
      (s"$db.${table}_use2", "table"),
      (s"$db.${table}_select1", "table"),
      (s"$db.${table}_select2", "table"),
      (s"$db.${table}_select3", "table"),
      (s"$db", "database"))) {
      runSqlAs("admin")(s"CREATE DATABASE IF NOT EXISTS $db")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.${table}_use1 (key int) USING $format")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.${table}_use2 (key int) USING $format")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.${table}_select1 (key int) USING $format")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.${table}_select2 (key int) USING $format")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.${table}_select3 (key int) USING $format")

      runSqlAs("admin")(s"show table extended from $db like '$table*'", collectSize = 5)
      runSqlAs("bob")(s"show tables from $db", collectSize = 5)
      runSqlAs("bob")(s"show table extended from $db like '$table*'", collectSize = 3)
      runSqlAs("i_am_invisible")(s"show table extended from $db like '$table*'", collectSize = 0)
    }
  }

  test("[KYUUBI #3430] AlterTableRenameCommand should skip permission check if it's tempview") {
    val tempView = "temp_view"
    val tempView2 = "temp_view2"
    val globalTempView = "global_temp_view"
    val globalTempView2 = "global_temp_view2"

    // create or replace view
    runSqlAs("denyuser")(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)")
    runSqlAs("denyuser")(s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)")

    // rename view
    runSqlAs("denyuser2")(s"ALTER VIEW $tempView RENAME TO $tempView2")
    runSqlAs("denyuser2")(
      s"ALTER VIEW global_temp.$globalTempView RENAME TO global_temp.$globalTempView2")

    runSqlAs("admin")(s"DROP VIEW IF EXISTS $tempView2")
    runSqlAs("admin")(s"DROP VIEW IF EXISTS global_temp.$globalTempView2")
    runSqlAs("admin")("show tables from global_temp", collectSize = 0)
  }

  test("[KYUUBI #3426] Drop temp view should be skipped permission check") {
    val tempView = "temp_view"
    val globalTempView = "global_temp_view"
    runSqlAs("denyuser")(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)")
    runSqlAs("denyuser")(s"CREATE OR REPLACE TEMPORARY VIEW $tempView AS select * from values(1)")

    runSqlAs("denyuser")(s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)")
    runSqlAs("denyuser")(s"CREATE OR REPLACE GLOBAL TEMPORARY VIEW $globalTempView" +
      s" AS select * from values(1)")

    // global_temp will contain the temporary view, even if it is not global
    runSqlAs("admin")("show tables from global_temp", collectSize = 2)

    runSqlAs("denyuser2")(s"DROP VIEW IF EXISTS $tempView")
    runSqlAs("denyuser2")(s"DROP VIEW IF EXISTS global_temp.$globalTempView")

    runSqlAs("admin")("show tables from global_temp", collectSize = 0)
  }

  test("[KYUUBI #3428] AlterViewAsCommand should be skipped permission check") {
    val tempView = "temp_view"
    val globalTempView = "global_temp_view"

    // create or replace view
    runSqlAs("denyuser")(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)")
    runSqlAs("denyuser")(s"CREATE OR REPLACE TEMPORARY VIEW $tempView" +
      s" AS select * from values(1)")
    runSqlAs("denyuser")(s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)")
    runSqlAs("denyuser")(s"CREATE OR REPLACE GLOBAL TEMPORARY VIEW $globalTempView" +
      s" AS select * from values(1)")

    // rename view
    runSqlAs("denyuser2")(s"ALTER VIEW $tempView AS SELECT * FROM values(1)")
    runSqlAs("denyuser2")(s"ALTER VIEW global_temp.$globalTempView AS SELECT * FROM values(1)")

    runSqlAs("admin")(s"DROP VIEW IF EXISTS $tempView")
    runSqlAs("admin")(s"DROP VIEW IF EXISTS global_temp.$globalTempView")
    runSqlAs("admin")("show tables from global_temp", collectSize = 0)
  }

  test("[KYUUBI #3343] pass temporary view creation") {
    val tempView = "temp_view"
    val globalTempView = "global_temp_view"

    withTempView(tempView) {
      runSqlAs("denyuser")(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)")
      runSqlAs("denyuser")(
        s"CREATE OR REPLACE TEMPORARY VIEW $tempView AS select * from values(1)")
    }

    withGlobalTempView(globalTempView) {
      runSqlAs("denyuser")(
        s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)")
      runSqlAs("denyuser")(
        s"CREATE OR REPLACE GLOBAL TEMPORARY VIEW $globalTempView AS select * from values(1)")
    }

    runSqlAs("admin")("show tables from global_temp", collectSize = 0)
  }
}

class InMemoryCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "in-memory"
}

class HiveCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
  test("table stats must be specified") {
    val table = "hive_src"
    withCleanTmpResources(Seq((table, "table"))) {
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $table (id int)")
      doAs("admin") {
        val hiveTableRelation = sql(s"SELECT * FROM $table")
          .queryExecution.optimizedPlan.collectLeaves().head.asInstanceOf[HiveTableRelation]
        assert(getFieldVal[Option[Statistics]](hiveTableRelation, "tableStats").nonEmpty)
      }
    }
  }

  test("HiveTableRelation should be able to be converted to LogicalRelation") {
    val table = "hive_src"
    withCleanTmpResources(Seq((table, "table"))) {
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $table (id int) STORED AS PARQUET")
      doAs("admin") {
        val relation = sql(s"SELECT * FROM $table")
          .queryExecution.optimizedPlan.collectLeaves().head
        assert(relation.isInstanceOf[LogicalRelation])
      }
    }
  }

  test("Pass through JoinSelection") {
    val db = "test"
    val table1 = "table1"
    val table2 = "table2"

    withCleanTmpResources(Seq(
      (s"$db.$table2", "table"),
      (s"$db.$table1", "table"),
      (s"$db", "database"))) {
      doAs("admin") {
        sql(s"CREATE DATABASE IF NOT EXISTS $db")
        sql(s"CREATE TABLE IF NOT EXISTS $db.$table1(id int) STORED AS PARQUET")
        sql(s"INSERT INTO $db.$table1 SELECT 1")
        sql(s"CREATE TABLE IF NOT EXISTS $db.$table2(id int, name string) STORED AS PARQUET")
        sql(s"INSERT INTO $db.$table2 SELECT 1, 'a'")
        val join = s"SELECT a.id, b.name FROM $db.$table1 a JOIN $db.$table2 b ON a.id=b.id"
        assert(sql(join).collect().length == 1)
      }
    }
  }

  test("[KYUUBI #3343] check persisted view creation") {
    val table = "hive_src"
    val adminPermView = "admin_perm_view"
    val permView = "perm_view"

    withCleanTmpResources(Seq(
      (adminPermView, "view"),
      (permView, "view"),
      (table, "table"))) {
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $table (id int)")

      runSqlAs("admin")(s"CREATE VIEW ${adminPermView} AS SELECT * FROM $table")

      runSqlAsWithAccessException()(
        s"CREATE VIEW $permView AS SELECT 1 as a",
        contains = s"does not have [create] privilege on [default/$permView]")

      val errorMsg = if (isSparkV32OrGreater) {
        s"does not have [select] privilege on [default/$table/id]"
      } else {
        s"does not have [select] privilege on [$table]"
      }
      runSqlAsWithAccessException()(
        s"CREATE VIEW $permView AS SELECT * FROM $table",
        contains = errorMsg)
    }
  }

  test("[KYUUBI #3326] check persisted view and skip shadowed table") {
    val table = "hive_src"
    val permView = "perm_view"
    val db1 = "default"
    val db2 = "db2"

    withCleanTmpResources(Seq(
      (s"$db1.$table", "table"),
      (s"$db2.$permView", "view"),
      (db2, "database"))) {
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db1.$table (id int)")

      runSqlAs("admin")(s"CREATE DATABASE IF NOT EXISTS $db2")
      runSqlAs("admin")(s"CREATE VIEW $db2.$permView AS SELECT * FROM $table")

      val errorMsg = if (isSparkV31OrGreater) {
        s"does not have [select] privilege on [$db2/$permView/id]"
      } else {
        s"does not have [select] privilege on [$db1/$table/id]"
      }
      runSqlAsWithAccessException()(
        s"select * from $db2.$permView",
        isCollect = true,
        contains = errorMsg)
    }
  }

  test("[KYUUBI #3371] support throws all disallowed privileges in exception") {
    val db1 = "default"
    val srcTable1 = "hive_src1"
    val srcTable2 = "hive_src2"
    val sinkTable1 = "hive_sink1"

    withCleanTmpResources(Seq(
      (s"$db1.$srcTable1", "table"),
      (s"$db1.$srcTable2", "table"),
      (s"$db1.$sinkTable1", "table"))) {
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db1.$srcTable1" +
        s" (id int, name string, city string)")

      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db1.$srcTable2" +
        s" (id int, age int)")

      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db1.$sinkTable1" +
        s" (id int, age int, name string, city string)")

      val insertSql1 = s"INSERT INTO $sinkTable1" +
        s" SELECT tb1.id as id, tb2.age as age, tb1.name as name, tb1.city as city" +
        s" FROM $db1.$srcTable1 as tb1" +
        s" JOIN $db1.$srcTable2 as tb2" +
        s" on tb1.id = tb2.id"
      runSqlAsWithAccessException()(
        insertSql1,
        contains = s"does not have [select] privilege on [$db1/$srcTable1/id]")

      try {
        SparkRangerAdminPlugin.getRangerConf.setBoolean(
          s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
          true)
        runSqlAsWithAccessException()(
          insertSql1,
          contains = s"does not have" +
            s" [select] privilege on" +
            s" [$db1/$srcTable1/id,$db1/$srcTable1/name,$db1/$srcTable1/city," +
            s"$db1/$srcTable2/age,$db1/$srcTable2/id]," +
            s" [update] privilege on [$db1/$sinkTable1/id,$db1/$sinkTable1/age," +
            s"$db1/$sinkTable1/name,$db1/$sinkTable1/city]")
      } finally {
        // revert to default value
        SparkRangerAdminPlugin.getRangerConf.setBoolean(
          s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
          false)
      }
    }
  }

  test("[KYUUBI #3411] skip checking cache table") {
    if (isSparkV32OrGreater) { // cache table sql supported since 3.2.0

      val db1 = "default"
      val srcTable1 = "hive_src1"
      val cacheTable1 = "cacheTable1"
      val cacheTable2 = "cacheTable2"
      val cacheTable3 = "cacheTable3"
      val cacheTable4 = "cacheTable4"

      withCleanTmpResources(Seq(
        (s"$db1.$srcTable1", "table"),
        (s"$db1.$cacheTable1", "cache"),
        (s"$db1.$cacheTable2", "cache"),
        (s"$db1.$cacheTable3", "cache"),
        (s"$db1.$cacheTable4", "cache"))) {

        runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db1.$srcTable1" +
          s" (id int, name string, city string)")

        runSqlAsWithAccessException()(
          s"CACHE TABLE $cacheTable2 select * from $db1.$srcTable1",
          contains = s"does not have [select] privilege on [$db1/$srcTable1/id]")

        runSqlAs("admin")(s"CACHE TABLE $cacheTable3 SELECT 1 AS a, 2 AS b ")
        runSqlAs("someone")(s"CACHE TABLE $cacheTable4 select 1 as a, 2 as b ")
      }
    }
  }

  test("[KYUUBI #3608] Support {OWNER} variable for queries") {
    val db = "default"
    val table = "owner_variable"

    val select = s"SELECT key FROM $db.$table"

    withCleanTmpResources(Seq((s"$db.$table", "table"))) {
      runSqlAs(defaultTableOwner)(s"CREATE TABLE $db.$table (key int, value int) USING $format")

      runSqlAs(defaultTableOwner)(select, isCollect = true)

      runSqlAsWithAccessException("create_only_user")(
        select,
        isCollect = true,
        contains = errorMessage("select", s"$db/$table/key"))
    }
  }

  test("modified query plan should correctly report stats") {
    val db = "stats_test"
    val table = "stats"
    withCleanTmpResources(
      Seq(
        (s"$db.$table", "table"),
        (s"$db", "database"))) {
      runSqlAs("admin")(s"CREATE DATABASE IF NOT EXISTS $db")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $db.$table (key int) USING $format")
      sql("SHOW DATABASES").queryExecution.optimizedPlan.stats
      sql(s"SHOW TABLES IN $db").queryExecution.optimizedPlan.stats
    }
  }
}
