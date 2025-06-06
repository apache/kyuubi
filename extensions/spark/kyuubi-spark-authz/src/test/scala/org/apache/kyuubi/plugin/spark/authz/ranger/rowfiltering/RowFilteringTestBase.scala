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

package org.apache.kyuubi.plugin.spark.authz.ranger.rowfiltering

// scalastyle:off
import scala.util.Try

import org.apache.spark.authz.AuthzConf
import org.apache.spark.authz.AuthzConf.ROW_FILTER_ENABLED
import org.apache.spark.sql.{Row, SparkSessionExtensions}
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider
import org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension

/**
 * Base trait for row filtering tests, derivative classes shall name themselves following:
 *  RowFilteringFor CatalogImpl?  FileFormat? Additions? Suite
 */
trait RowFilteringTestBase extends AnyFunSuite with SparkSessionProvider with BeforeAndAfterAll {
// scalastyle:on
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  private def setup(): Unit = {
    sql(s"CREATE TABLE IF NOT EXISTS default.src(key int, value int) $format")
    sql("INSERT INTO default.src SELECT 1, 1")
    sql("INSERT INTO default.src SELECT 20, 2")
    sql("INSERT INTO default.src SELECT 30, 3")
  }

  private def cleanup(): Unit = {
    sql("DROP TABLE IF EXISTS default.src")
  }

  override def beforeAll(): Unit = {
    doAs(admin, setup())
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    doAs(admin, cleanup())
    spark.stop
    super.afterAll()
  }

  private def withEnabledRowFilter(enabled: Boolean)(f: => Unit): Unit = {
    val conf = SQLConf.get
    val oldValue = AuthzConf.rowFilterEnabled(conf)
    try {
      conf.setConf(ROW_FILTER_ENABLED, enabled)
      f
    } finally {
      conf.setConf(ROW_FILTER_ENABLED, oldValue)
    }
  }

  test("user without row filtering rule") {
    checkAnswer(
      kent,
      "SELECT key FROM default.src order by key",
      Seq(Row(1), Row(20), Row(30)))
  }

  test("simple query projecting filtering column") {
    Seq(true, false).foreach { enabled =>
      withEnabledRowFilter(enabled) {
        val result = if (enabled) {
          Seq(Row(1))
        } else {
          Seq(Row(1), Row(20), Row(30))
        }
        checkAnswer(bob, "SELECT key FROM default.src order by key", result)
      }
    }
  }

  test("simple query projecting non filtering column") {
    Seq(true, false).foreach { enabled =>
      withEnabledRowFilter(enabled) {
        val result = if (enabled) {
          Seq(Row(1))
        } else {
          Seq(Row(1), Row(2), Row(3))
        }
        checkAnswer(bob, "SELECT value FROM default.src order by key", result)
      }
    }
  }

  test("simple query projecting non filtering column with udf max") {
    checkAnswer(bob, "SELECT max(value) FROM default.src", Seq(Row(1)))
  }

  test("simple query projecting non filtering column with udf coalesce") {
    checkAnswer(bob, "SELECT coalesce(max(value), 1) FROM default.src", Seq(Row(1)))
  }

  test("in subquery") {
    checkAnswer(
      bob,
      "SELECT value FROM default.src WHERE value in (SELECT value as key FROM default.src)",
      Seq(Row(1)))
  }

  test("ctas") {
    withCleanTmpResources(Seq(("default.src2", "table"))) {
      doAs(bob, sql(s"CREATE TABLE default.src2 $format AS SELECT value FROM default.src"))
      val query = "select value from default.src2"
      checkAnswer(admin, query, Seq(Row(1)))
      checkAnswer(bob, query, Seq(Row(1)))
    }
  }

  test("[KYUUBI #3581]: row level filter on permanent view") {
    val supported = doAs(
      permViewUser,
      Try(sql("CREATE OR REPLACE VIEW default.perm_view AS SELECT * FROM default.src")).isSuccess)
    assume(supported, s"view support for '$format' has not been implemented yet")

    withCleanTmpResources(Seq((s"default.perm_view", "view"))) {
      checkAnswer(
        admin,
        "SELECT key FROM default.perm_view order order by key",
        Seq(Row(1), Row(20), Row(30)))
      checkAnswer(bob, "SELECT key FROM default.perm_view", Seq(Row(1)))
      checkAnswer(bob, "SELECT value FROM default.perm_view", Seq(Row(1)))
      checkAnswer(bob, "SELECT max(value) FROM default.perm_view", Seq(Row(1)))
      checkAnswer(bob, "SELECT coalesce(max(value), 1) FROM default.perm_view", Seq(Row(1)))
      checkAnswer(
        bob,
        "SELECT value FROM default.perm_view WHERE value in " +
          "(SELECT value as key FROM default.perm_view)",
        Seq(Row(1)))
    }
  }
}
