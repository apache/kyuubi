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
import org.scalatest.BeforeAndAfterAll
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider

abstract class RangerSparkExtensionSuite extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll {
// scalastyle:on
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
      user: String = UserGroupInformation.getCurrentUser.getShortUserName): String = {
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

  test("data masking") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create =
      s"CREATE TABLE IF NOT EXISTS $db.$table" +
        s" ($col int, value1 int, value2 string, value3 string, value4 timestamp, value5 string)" +
        s" USING $format"
    try {
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
    } finally {
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}2"))
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.$table"))
    }
  }

  test("KYUUBI #2390: RuleEliminateMarker stays in analyze phase for data masking") {
    val db = "default"
    val table = "src"
    val create =
      s"CREATE TABLE IF NOT EXISTS $db.$table (key int, value1 int) USING $format"
    try {
      doAs("admin", sql(create))
      doAs("admin", sql(s"INSERT INTO $db.$table SELECT 1, 1"))
      // scalastyle: off
      doAs(
        "bob", {
          assert(sql(s"select * from $db.$table").collect() ===
            Seq(Row(1, DigestUtils.md5Hex("1"))))
          assert(Try(sql(s"select * from $db.$table").show(1)).isSuccess)
        })
    } finally {
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.$table"))
    }
  }

  test("show tables") {
    val db = "default2"
    val table = "src"
    try {
      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $db"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.$table (key int) USING $format"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $db.${table}for_show (key int) USING $format"))

      doAs("admin", assert(sql(s"show tables from $db").collect().length === 2))
      doAs("bob", assert(sql(s"show tables from $db").collect().length === 0))
      doAs("i_am_invisible", assert(sql(s"show tables from $db").collect().length === 0))
    } finally {
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.$table"))
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}for_show"))
      doAs("admin", sql(s"DROP DATABASE IF EXISTS $db"))
    }
  }

  test("show databases") {
    val db = "default2"
    try {
      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $db"))
      doAs("admin", assert(sql(s"SHOW DATABASES").collect().length == 2))
      doAs("admin", assert(sql(s"SHOW DATABASES").collectAsList().get(0).getString(0) == "default"))
      doAs("admin", assert(sql(s"SHOW DATABASES").collectAsList().get(1).getString(0) == s"$db"))

      doAs("bob", assert(sql(s"SHOW DATABASES").collect().length == 1))
      doAs("bob", assert(sql(s"SHOW DATABASES").collectAsList().get(0).getString(0) == "default"))
    } finally {
      doAs("admin", sql(s"DROP DATABASE IF EXISTS $db"))
    }
  }

  test("show functions") {
    val default = "default"
    val db3 = "default3"
    val function1 = "function1"
    try {
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
    } finally {
      doAs("admin", sql(s"DROP FUNCTION IF EXISTS $default.$function1"))
      doAs("admin", sql(s"DROP FUNCTION IF EXISTS $db3.$function1"))
      doAs("admin", sql(s"DROP DATABASE IF EXISTS $db3"))
    }
  }

  test("show columns") {
    val db = "default"
    val table = "src"
    val col = "key"
    val create = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING $format"
    try {
      doAs("admin", sql(create))

      doAs("admin", assert(sql(s"SHOW COLUMNS IN $table").count() == 2))
      doAs("admin", assert(sql(s"SHOW COLUMNS IN $db.$table").count() == 2))
      doAs("admin", assert(sql(s"SHOW COLUMNS IN $table IN $db").count() == 2))

      doAs("kent", assert(sql(s"SHOW COLUMNS IN $table").count() == 1))
      doAs("kent", assert(sql(s"SHOW COLUMNS IN $db.$table").count() == 1))
      doAs("kent", assert(sql(s"SHOW COLUMNS IN $table IN $db").count() == 1))
    } finally {
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.$table"))
    }
  }

  test("show table extended") {
    val db = "default_bob"
    val table = "table"
    try {
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
    } finally {
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}_use1"))
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}_use2"))
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}_select1"))
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}_select2"))
      doAs("admin", sql(s"DROP TABLE IF EXISTS $db.${table}_select3"))
      doAs("admin", sql(s"DROP DATABASE IF EXISTS $db"))
    }
  }
}

class InMemoryCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "in-memory"
}

class HiveCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
}
