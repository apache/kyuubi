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

import java.security.PrivilegedExceptionAction
import java.sql.Timestamp

import scala.util.Try

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.scalatest.BeforeAndAfterAll
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization.KYUUBI_AUTHZ_TAG
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.getFieldVal

abstract class RangerSparkExtensionSuite extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll {
  // scalastyle:on
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  protected def doAs[T](user: String, f: => T): T = {
    UserGroupInformation.createRemoteUser(user).doAs[T](
      new PrivilegedExceptionAction[T] {
        override def run(): T = f
      })
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  protected def errorMessage(
      privilege: String,
      resource: String = "default/src",
      user: String = UserGroupInformation.getCurrentUser.getShortUserName): String = {
    s"Permission denied: user [$user] does not have [$privilege] privilege on [$resource]"
  }

  protected def withCleanTmpResources[T](res: Seq[(String, String)])(f: => T): T = {
    try {
      f
    } finally {
      res.foreach {
        case (t, "table") => doAs("admin", sql(s"DROP TABLE IF EXISTS $t"))
        case (db, "database") => doAs("admin", sql(s"DROP DATABASE IF EXISTS $db"))
        case (fn, "function") => doAs("admin", sql(s"DROP FUNCTION IF EXISTS $fn"))
        case (view, "view") => doAs("admin", sql(s"DROP VIEW IF EXISTS $view"))
        case (cacheTable, "cache") => if (isSparkV32OrGreater) {
            doAs("admin", sql(s"UNCACHE TABLE IF EXISTS $cacheTable"))
          }
        case (_, e) =>
          throw new RuntimeException(s"the resource whose resource type is $e cannot be cleared")
      }
    }
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
    val logicalPlan = doAs("admin", sql("SHOW TABLES").queryExecution.logical)
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
      doAs(
        "admin", {
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
        })
    }
  }

  test("auth: databases") {
    val testDb = "mydb"
    val create = s"CREATE DATABASE IF NOT EXISTS $testDb"
    val alter = s"ALTER DATABASE $testDb SET DBPROPERTIES (abc = '123')"
    val drop = s"DROP DATABASE IF EXISTS $testDb"

    val e = intercept[AccessControlException](sql(create))
    assert(e.getMessage === errorMessage("create", "mydb"))
    withCleanTmpResources(Seq((testDb, "database"))) {
      doAs("admin", assert(Try { sql(create) }.isSuccess))
      doAs("admin", assert(Try { sql(alter) }.isSuccess))
      val e1 = intercept[AccessControlException](sql(alter))
      assert(e1.getMessage === errorMessage("alter", "mydb"))
      val e2 = intercept[AccessControlException](sql(drop))
      assert(e2.getMessage === errorMessage("drop", "mydb"))
      doAs("kent", Try(sql("SHOW DATABASES")).isSuccess)
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
    val e = intercept[AccessControlException](sql(create0))
    assert(e.getMessage === errorMessage("create"))

    withCleanTmpResources(Seq((s"$db.$table", "table"))) {
      doAs("bob", assert(Try { sql(create0) }.isSuccess))
      doAs("bob", assert(Try { sql(alter0) }.isSuccess))

      val e1 = intercept[AccessControlException](sql(drop0))
      assert(e1.getMessage === errorMessage("drop"))
      doAs("bob", assert(Try { sql(alter0) }.isSuccess))
      doAs("bob", assert(Try { sql(select).collect() }.isSuccess))
      doAs("kent", assert(Try { sql(s"SELECT key FROM $db.$table").collect() }.isSuccess))

      Seq(
        select,
        s"SELECT value FROM $db.$table",
        s"SELECT value as key FROM $db.$table",
        s"SELECT max(value) FROM $db.$table",
        s"SELECT coalesce(max(value), 1) FROM $db.$table",
        s"SELECT key FROM $db.$table WHERE value in (SELECT value as key FROM $db.$table)")
        .foreach { q =>
          doAs(
            "kent", {
              withClue(q) {
                val e = intercept[AccessControlException](sql(q).collect())
                assert(e.getMessage === errorMessage("select", "default/src/value", "kent"))
              }
            })
        }
    }
  }

  test("auth: functions") {
    val db = "default"
    val func = "func"
    val create0 = s"CREATE FUNCTION IF NOT EXISTS $db.$func AS 'abc.mnl.xyz'"
    doAs(
      "kent", {
        val e = intercept[AccessControlException](sql(create0))
        assert(e.getMessage === errorMessage("create", "default/func"))
      })
    doAs("admin", assert(Try(sql(create0)).isSuccess))
  }

  test("row level filter") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"

    withCleanTmpResources(Seq((s"$db.${table}2", "table"), (s"$db.$table", "table"))) {
      doAs("admin", assert(Try { sql(create) }.isSuccess))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 1, 1"))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 20, 2"))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 30, 3"))

      doAs(
        "kent",
        assert(sql(s"SELECT key FROM $db.$table order by key").collect() ===
          Seq(Row(1), Row(20), Row(30))))

      Seq(
        s"SELECT value FROM $db.$table",
        s"SELECT value as key FROM $db.$table",
        s"SELECT max(value) FROM $db.$table",
        s"SELECT coalesce(max(value), 1) FROM $db.$table",
        s"SELECT value FROM $db.$table WHERE value in (SELECT value as key FROM $db.$table)")
        .foreach { q =>
          doAs(
            "bob", {
              withClue(q) {
                assert(sql(q).collect() === Seq(Row(1)))
              }
            })
        }
      doAs(
        "bob", {
          sql(s"CREATE TABLE $db.src2 using $format AS SELECT value FROM $db.$table")
          assert(sql(s"SELECT value FROM $db.${table}2").collect() === Seq(Row(1)))
        })
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
      doAs("admin", assert(Try { sql(create) }.isSuccess))
      doAs("admin", assert(Try { sql(createView) }.isSuccess))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 1, 1"))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 20, 2"))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 30, 3"))

      Seq(
        s"SELECT value FROM $db.$permView",
        s"SELECT value as key FROM $db.$permView",
        s"SELECT max(value) FROM $db.$permView",
        s"SELECT coalesce(max(value), 1) FROM $db.$permView",
        s"SELECT value FROM $db.$permView WHERE value in (SELECT value as key FROM $db.$permView)")
        .foreach { q =>
          doAs(
            "perm_view_user", {
              withClue(q) {
                assert(sql(q).collect() === Seq(Row(1)))
              }
            })
        }
    }
  }

  test("data masking") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create =
      s"CREATE TABLE IF NOT EXISTS $db.$table" +
        s" ($col int, value1 int, value2 string, value3 string, value4 timestamp, value5 string)" +
        s" USING $format"

    withCleanTmpResources(Seq(
      (s"$db.${table}2", "table"),
      (s"$db.$table", "table"))) {
      doAs("admin", assert(Try { sql(create) }.isSuccess))
      doAs(
        "admin",
        sql(
          s"INSERT INTO $db.$table SELECT 1, 1, 'hello', 'world', " +
            s"timestamp'2018-11-17 12:34:56', 'World'"))
      doAs(
        "admin",
        sql(
          s"INSERT INTO $db.$table SELECT 20, 2, 'kyuubi', 'y', " +
            s"timestamp'2018-11-17 12:34:56', 'world'"))
      doAs(
        "admin",
        sql(
          s"INSERT INTO $db.$table SELECT 30, 3, 'spark', 'a'," +
            s" timestamp'2018-11-17 12:34:56', 'world'"))

      doAs(
        "kent",
        assert(sql(s"SELECT key FROM $db.$table order by key").collect() ===
          Seq(Row(1), Row(20), Row(30))))

      Seq(
        s"SELECT value1, value2, value3, value4, value5 FROM $db.$table",
        s"SELECT value1 as key, value2, value3, value4, value5 FROM $db.$table",
        s"SELECT max(value1), max(value2), max(value3), max(value4), max(value5) FROM $db.$table",
        s"SELECT coalesce(max(value1), 1), coalesce(max(value2), 1), coalesce(max(value3), 1), " +
          s"coalesce(max(value4), timestamp '2018-01-01 22:33:44'), coalesce(max(value5), 1) " +
          s"FROM $db.$table",
        s"SELECT value1, value2, value3, value4, value5 FROM $db.$table WHERE value2 in" +
          s" (SELECT value2 as key FROM $db.$table)")
        .foreach { q =>
          doAs(
            "bob", {
              withClue(q) {
                assert(sql(q).collect() ===
                  Seq(
                    Row(
                      DigestUtils.md5Hex("1"),
                      "xxxxx",
                      "worlx",
                      Timestamp.valueOf("2018-01-01 00:00:00"),
                      "Xorld")))
              }
            })
        }
      doAs(
        "bob", {
          sql(s"CREATE TABLE $db.src2 using $format AS SELECT value1 FROM $db.$table")
          assert(sql(s"SELECT value1 FROM $db.${table}2").collect() ===
            Seq(Row(DigestUtils.md5Hex("1"))))
        })
    }
  }

  test("[KYUUBI #3581]: data masking on permanent view") {
    assume(isSparkV31OrGreater)

    val db = "default"
    val table = "src"
    val permView = "perm_view"
    val col = "key"
    val create =
      s"CREATE TABLE IF NOT EXISTS $db.$table" +
        s" ($col int, value1 int, value2 string)" +
        s" USING $format"

    val createView =
      s"CREATE OR REPLACE VIEW $db.$permView" +
        s" AS SELECT * FROM $db.$table"

    withCleanTmpResources(Seq(
      (s"$db.$table", "table"),
      (s"$db.$permView", "view"))) {
      doAs("admin", assert(Try { sql(create) }.isSuccess))
      doAs("admin", assert(Try { sql(createView) }.isSuccess))
      doAs(
        "admin",
        sql(
          s"INSERT INTO $db.$table SELECT 1, 1, 'hello'"))

      Seq(
        s"SELECT value1, value2 FROM $db.$permView")
        .foreach { q =>
          doAs(
            "perm_view_user", {
              withClue(q) {
                assert(sql(q).collect() ===
                  Seq(
                    Row(
                      DigestUtils.md5Hex("1"),
                      "hello")))
              }
            })
        }
    }
  }

  test("KYUUBI #2390: RuleEliminateMarker stays in analyze phase for data masking") {
    val db = "default"
    val table = "src"
    val create =
      s"CREATE TABLE IF NOT EXISTS $db.$table (key int, value1 int) USING $format"

    withCleanTmpResources(Seq((s"$db.$table", "table"))) {
      doAs("admin", sql(create))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 1, 1"))
      // scalastyle: off
      doAs(
        "bob", {
          assert(sql(s"select * from $db.$table").collect() ===
            Seq(Row(1, DigestUtils.md5Hex("1"))))
          assert(Try(sql(s"select * from $db.$table").show(1)).isSuccess)
        })
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
      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $db"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.$table (key int) USING $format"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.${table}for_show (key int) USING $format"))

      doAs("admin", assert(sql(s"show tables from $db").collect().length === 2))
      doAs("bob", assert(sql(s"show tables from $db").collect().length === 0))
      doAs("i_am_invisible", assert(sql(s"show tables from $db").collect().length === 0))
    }
  }

  test("show databases") {
    val db = "default2"

    withCleanTmpResources(Seq((db, "database"))) {
      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $db"))
      doAs("admin", assert(sql(s"SHOW DATABASES").collect().length == 2))
      doAs("admin", assert(sql(s"SHOW DATABASES").collectAsList().get(0).getString(0) == "default"))
      doAs("admin", assert(sql(s"SHOW DATABASES").collectAsList().get(1).getString(0) == s"$db"))

      doAs("bob", assert(sql(s"SHOW DATABASES").collect().length == 1))
      doAs("bob", assert(sql(s"SHOW DATABASES").collectAsList().get(0).getString(0) == "default"))
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
      doAs("admin", sql(s"CREATE FUNCTION $function1 AS 'Function1'"))
      doAs("admin", assert(sql(s"show user functions $default.$function1").collect().length == 1))
      doAs("bob", assert(sql(s"show user functions $default.$function1").collect().length == 0))

      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $db3"))
      doAs("admin", sql(s"CREATE FUNCTION $db3.$function1 AS 'Function1'"))

      doAs("admin", assert(sql(s"show user functions $db3.$function1").collect().length == 1))
      doAs("bob", assert(sql(s"show user functions $db3.$function1").collect().length == 0))

      doAs("admin", assert(sql(s"show system functions").collect().length > 0))
      doAs("bob", assert(sql(s"show system functions").collect().length > 0))

      val adminSystemFunctionCount = doAs("admin", sql(s"show system functions").collect().length)
      val bobSystemFunctionCount = doAs("bob", sql(s"show system functions").collect().length)
      assert(adminSystemFunctionCount == bobSystemFunctionCount)
    }
  }

  test("show columns") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"

    withCleanTmpResources(Seq((s"$db.$table", "table"))) {
      doAs("admin", sql(create))

      doAs("admin", assert(sql(s"SHOW COLUMNS IN $table").count() == 2))
      doAs("admin", assert(sql(s"SHOW COLUMNS IN $db.$table").count() == 2))
      doAs("admin", assert(sql(s"SHOW COLUMNS IN $table IN $db").count() == 2))

      doAs("kent", assert(sql(s"SHOW COLUMNS IN $table").count() == 1))
      doAs("kent", assert(sql(s"SHOW COLUMNS IN $db.$table").count() == 1))
      doAs("kent", assert(sql(s"SHOW COLUMNS IN $table IN $db").count() == 1))
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
      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $db"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.${table}_use1 (key int) USING $format"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.${table}_use2 (key int) USING $format"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.${table}_select1 (key int) USING $format"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.${table}_select2 (key int) USING $format"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.${table}_select3 (key int) USING $format"))

      doAs(
        "admin",
        assert(sql(s"show table extended from $db like '$table*'").collect().length === 5))
      doAs(
        "bob",
        assert(sql(s"show tables from $db").collect().length === 5))
      doAs(
        "bob",
        assert(sql(s"show table extended from $db like '$table*'").collect().length === 3))
      doAs(
        "i_am_invisible",
        assert(sql(s"show table extended from $db like '$table*'").collect().length === 0))
    }
  }

  test("[KYUUBI #3430] AlterTableRenameCommand should skip permission check if it's tempview") {
    val tempView = "temp_view"
    val tempView2 = "temp_view2"
    val globalTempView = "global_temp_view"
    val globalTempView2 = "global_temp_view2"

    // create or replace view
    doAs("denyuser", sql(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)"))
    doAs(
      "denyuser",
      sql(s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)"))

    // rename view
    doAs("denyuser2", sql(s"ALTER VIEW $tempView RENAME TO $tempView2"))
    doAs(
      "denyuser2",
      sql(s"ALTER VIEW global_temp.$globalTempView RENAME TO global_temp.$globalTempView2"))

    doAs("admin", sql(s"DROP VIEW IF EXISTS $tempView2"))
    doAs("admin", sql(s"DROP VIEW IF EXISTS global_temp.$globalTempView2"))
    doAs("admin", assert(sql("show tables from global_temp").collect().length == 0))
  }

  test("[KYUUBI #3426] Drop temp view should be skipped permission check") {
    val tempView = "temp_view"
    val globalTempView = "global_temp_view"
    doAs("denyuser", sql(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)"))

    doAs(
      "denyuser",
      sql(s"CREATE OR REPLACE TEMPORARY VIEW $tempView" +
        s" AS select * from values(1)"))

    doAs(
      "denyuser",
      sql(s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)"))

    doAs(
      "denyuser",
      sql(s"CREATE OR REPLACE GLOBAL TEMPORARY VIEW $globalTempView" +
        s" AS select * from values(1)"))

    // global_temp will contain the temporary view, even if it is not global
    doAs("admin", assert(sql("show tables from global_temp").collect().length == 2))

    doAs("denyuser2", sql(s"DROP VIEW IF EXISTS $tempView"))
    doAs("denyuser2", sql(s"DROP VIEW IF EXISTS global_temp.$globalTempView"))

    doAs("admin", assert(sql("show tables from global_temp").collect().length == 0))
  }

  test("[KYUUBI #3428] AlterViewAsCommand should be skipped permission check") {
    val tempView = "temp_view"
    val globalTempView = "global_temp_view"

    // create or replace view
    doAs("denyuser", sql(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)"))
    doAs(
      "denyuser",
      sql(s"CREATE OR REPLACE TEMPORARY VIEW $tempView" +
        s" AS select * from values(1)"))
    doAs(
      "denyuser",
      sql(s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)"))
    doAs(
      "denyuser",
      sql(s"CREATE OR REPLACE GLOBAL TEMPORARY VIEW $globalTempView" +
        s" AS select * from values(1)"))

    // rename view
    doAs("denyuser2", sql(s"ALTER VIEW $tempView AS SELECT * FROM values(1)"))
    doAs("denyuser2", sql(s"ALTER VIEW global_temp.$globalTempView AS SELECT * FROM values(1)"))

    doAs("admin", sql(s"DROP VIEW IF EXISTS $tempView"))
    doAs("admin", sql(s"DROP VIEW IF EXISTS global_temp.$globalTempView"))
    doAs("admin", assert(sql("show tables from global_temp").collect().length == 0))
  }

  test("[KYUUBI #3343] pass temporary view creation") {
    val tempView = "temp_view"
    val globalTempView = "global_temp_view"

    withTempView(tempView) {
      doAs(
        "denyuser",
        assert(Try(sql(s"CREATE TEMPORARY VIEW $tempView AS select * from values(1)")).isSuccess))

      doAs(
        "denyuser",
        Try(sql(s"CREATE OR REPLACE TEMPORARY VIEW $tempView" +
          s" AS select * from values(1)")).isSuccess)
    }

    withGlobalTempView(globalTempView) {
      doAs(
        "denyuser",
        Try(
          sql(
            s"CREATE GLOBAL TEMPORARY VIEW $globalTempView AS SELECT * FROM values(1)")).isSuccess)

      doAs(
        "denyuser",
        Try(sql(s"CREATE OR REPLACE GLOBAL TEMPORARY VIEW $globalTempView" +
          s" AS select * from values(1)")).isSuccess)
    }
    doAs("admin", assert(sql("show tables from global_temp").collect().length == 0))
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
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $table (id int)"))
      doAs(
        "admin", {
          val hiveTableRelation = sql(s"SELECT * FROM $table")
            .queryExecution.optimizedPlan.collectLeaves().head.asInstanceOf[HiveTableRelation]
          assert(getFieldVal[Option[Statistics]](hiveTableRelation, "tableStats").nonEmpty)
        })
    }
  }

  test("HiveTableRelation should be able to be converted to LogicalRelation") {
    val table = "hive_src"
    withCleanTmpResources(Seq((table, "table"))) {
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $table (id int) STORED AS PARQUET"))
      doAs(
        "admin", {
          val relation = sql(s"SELECT * FROM $table")
            .queryExecution.optimizedPlan.collectLeaves().head
          assert(relation.isInstanceOf[LogicalRelation])
        })
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
      doAs(
        "admin", {
          sql(s"CREATE DATABASE IF NOT EXISTS $db")
          sql(s"CREATE TABLE IF NOT EXISTS $db.$table1(id int) STORED AS PARQUET")
          sql(s"INSERT INTO $db.$table1 SELECT 1")
          sql(s"CREATE TABLE IF NOT EXISTS $db.$table2(id int, name string) STORED AS PARQUET")
          sql(s"INSERT INTO $db.$table2 SELECT 1, 'a'")
          val join = s"SELECT a.id, b.name FROM $db.$table1 a JOIN $db.$table2 b ON a.id=b.id"
          assert(sql(join).collect().length == 1)
        })
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
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $table (id int)"))

      doAs("admin", sql(s"CREATE VIEW ${adminPermView} AS SELECT * FROM $table"))

      val e1 = intercept[AccessControlException](
        doAs("someone", sql(s"CREATE VIEW $permView AS SELECT 1 as a")))
      assert(e1.getMessage.contains(s"does not have [create] privilege on [default/$permView]"))

      val e2 = intercept[AccessControlException](
        doAs("someone", sql(s"CREATE VIEW $permView AS SELECT * FROM $table")))
      if (isSparkV32OrGreater) {
        assert(e2.getMessage.contains(s"does not have [select] privilege on [default/$table/id]"))
      } else {
        assert(e2.getMessage.contains(s"does not have [select] privilege on [$table]"))
      }
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
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db1.$table (id int)"))

      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $db2"))
      doAs("admin", sql(s"CREATE VIEW $db2.$permView AS SELECT * FROM $table"))

      val e1 = intercept[AccessControlException](
        doAs("someone", sql(s"select * from $db2.$permView")).show(0))
      if (isSparkV31OrGreater) {
        assert(e1.getMessage.contains(s"does not have [select] privilege on [$db2/$permView/id]"))
      } else {
        assert(e1.getMessage.contains(s"does not have [select] privilege on [$db1/$table/id]"))
      }
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
      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $db1.$srcTable1" +
          s" (id int, name string, city string)"))

      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $db1.$srcTable2" +
          s" (id int, age int)"))

      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $db1.$sinkTable1" +
          s" (id int, age int, name string, city string)"))

      val insertSql1 = s"INSERT INTO $sinkTable1" +
        s" SELECT tb1.id as id, tb2.age as age, tb1.name as name, tb1.city as city" +
        s" FROM $db1.$srcTable1 as tb1" +
        s" JOIN $db1.$srcTable2 as tb2" +
        s" on tb1.id = tb2.id"
      val e1 = intercept[AccessControlException](doAs("someone", sql(insertSql1)))
      assert(e1.getMessage.contains(s"does not have [select] privilege on [$db1/$srcTable1/id]"))

      try {
        SparkRangerAdminPlugin.getRangerConf.setBoolean(
          s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
          true)
        val e2 = intercept[AccessControlException](doAs("someone", sql(insertSql1)))
        assert(e2.getMessage.contains(s"does not have" +
          s" [select] privilege on" +
          s" [$db1/$srcTable1/id,$db1/$srcTable1/name,$db1/$srcTable1/city," +
          s"$db1/$srcTable2/age,$db1/$srcTable2/id]," +
          s" [update] privilege on [$db1/$sinkTable1/id,$db1/$sinkTable1/age," +
          s"$db1/$sinkTable1/name,$db1/$sinkTable1/city]"))
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

        doAs(
          "admin",
          sql(s"CREATE TABLE IF NOT EXISTS $db1.$srcTable1" +
            s" (id int, name string, city string)"))

        val e1 = intercept[AccessControlException](
          doAs("someone", sql(s"CACHE TABLE $cacheTable2 select * from $db1.$srcTable1")))
        assert(
          e1.getMessage.contains(s"does not have [select] privilege on [$db1/$srcTable1/id]"))

        doAs("admin", sql(s"CACHE TABLE $cacheTable3 SELECT 1 AS a, 2 AS b "))
        doAs("someone", sql(s"CACHE TABLE $cacheTable4 select 1 as a, 2 as b "))
      }
    }
  }

  test("[KYUUBI #3608] Support {OWNER} variable for queries") {
    val db = "default"
    val table = "owner_variable"

    val select = s"SELECT key FROM $db.$table"

    withCleanTmpResources(Seq((s"$db.$table", "table"))) {
      doAs(
        defaultTableOwner,
        assert(Try {
          sql(s"CREATE TABLE $db.$table (key int, value int) USING $format")
        }.isSuccess))

      doAs(
        defaultTableOwner,
        assert(Try {
          sql(select).collect()
        }.isSuccess))

      doAs(
        "create_only_user", {
          val e = intercept[AccessControlException](sql(select).collect())
          assert(e.getMessage === errorMessage("select", s"$db/$table/key"))
        })
    }
  }
}
