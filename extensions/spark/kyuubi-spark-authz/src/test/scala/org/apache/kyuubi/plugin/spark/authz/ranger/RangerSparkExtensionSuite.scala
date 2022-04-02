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

import scala.util.Try

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSessionExtensions}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider

abstract class RangerSparkExtensionSuite extends KyuubiFunSuite with SparkSessionProvider {

  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension

  private def doAs[T](user: String, f: => T): T = {
    UserGroupInformation.createRemoteUser(user).doAs[T](
      new PrivilegedExceptionAction[T] {
        override def run(): T = f
      })
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  private def errorMessage(
      privilege: String,
      resource: String = "default/src",
      user: String = Utils.currentUser): String = {
    s"Permission denied: user [$user] does not have [$privilege] privilege on [$resource]"
  }

  test("auth: databases") {
    val testDb = "mydb"
    val create = s"CREATE DATABASE IF NOT EXISTS $testDb"
    val alter = s"ALTER DATABASE $testDb SET DBPROPERTIES (abc = '123')"
    val drop = s"DROP DATABASE IF EXISTS $testDb"

    val e = intercept[RuntimeException](sql(create))
    assert(e.getMessage === errorMessage("create", "mydb"))
    try {
      doAs("admin", assert(Try { sql(create) }.isSuccess))
      doAs("admin", assert(Try { sql(alter) }.isSuccess))
      val e1 = intercept[RuntimeException](sql(alter))
      assert(e1.getMessage === errorMessage("alter", "mydb"))
      val e2 = intercept[RuntimeException](sql(drop))
      assert(e2.getMessage === errorMessage("drop", "mydb"))
      doAs("kent", Try(sql("SHOW DATABASES")).isSuccess)
    } finally {
      doAs("admin", sql(drop))
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
    val e = intercept[RuntimeException](sql(create0))
    assert(e.getMessage === errorMessage("create"))

    try {
      doAs("bob", assert(Try { sql(create0) }.isSuccess))
      doAs("bob", assert(Try { sql(alter0) }.isSuccess))

      val e1 = intercept[RuntimeException](sql(drop0))
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
                val e = intercept[RuntimeException](sql(q).collect())
                assert(e.getMessage === errorMessage("select", "default/src/value", "kent"))
              }
            })
        }
    } finally {
      doAs("admin", sql(drop0))
    }
  }

  test("auth: functions") {
    val db = "default"
    val func = "func"
    val create0 = s"CREATE FUNCTION IF NOT EXISTS $db.$func AS 'abc.mnl.xyz'"
    doAs(
      "kent", {
        val e = intercept[RuntimeException](sql(create0))
        assert(e.getMessage === errorMessage("create", "default/func"))
      })
    doAs("admin", assert(Try(sql(create0)).isSuccess))
  }

  test("row level filter") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"
    try {
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
    } finally {
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}2"))
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.$table"))
    }
  }
}

class InMemoryCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "in-memory"
}

class HiveCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
}
