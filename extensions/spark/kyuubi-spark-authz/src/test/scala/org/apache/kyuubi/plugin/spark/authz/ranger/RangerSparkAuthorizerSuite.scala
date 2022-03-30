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
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}

class RangerSparkAuthorizerSuite extends KyuubiFunSuite {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .withExtensions(new RangerSparkExtension)
    .config("spark.ui.enabled", "false")
    .config(
      "spark.sql.warehouse.dir",
      Utils.createTempDir(namePrefix = "spark-warehouse").toString)
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()

  private val sql: String => DataFrame = spark.sql

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

  test("databases") {
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
      assert(e2.getMessage === (errorMessage("drop", "mydb")))
      doAs("kent", Try(sql("SHOW DATABASES")).isSuccess)
    } finally {
      doAs("admin", sql(drop))
    }
  }

  test("tables") {
    val db = "default"
    val table = "src"
    val col = "key"

    val create0 = s"CREATE TABLE IF NOT EXISTS $db.$table ($col int, value int) USING parquet"
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

  test("functions") {
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
}
