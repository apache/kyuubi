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
    UserGroupInformation.createRemoteUser("admin").doAs[T](
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
      user: String = Utils.currentUser): String = {
    s"Permission denied: user [$user] does not have [$privilege]"
  }

  test("databases") {
    val testDb = "mydb"
    val create = s"CREATE DATABASE IF NOT EXISTS $testDb"
    val alter = s"ALTER DATABASE $testDb SET DBPROPERTIES (abc = '123')"
    val drop = s"DROP DATABASE IF EXISTS $testDb"

    val e = intercept[RuntimeException](sql(create))
    assert(e.getMessage.startsWith(errorMessage("create")))
    try {
      doAs("admin", assert(Try { sql(create) }.isSuccess))
      doAs("admin", assert(Try { sql(alter) }.isSuccess))
      val e1 = intercept[RuntimeException](sql(alter))
      assert(e1.getMessage.startsWith(errorMessage("alter")))
      val e2 = intercept[RuntimeException](sql(drop))
      assert(e2.getMessage.startsWith(errorMessage("drop")))
    } finally {
      doAs("admin", sql(drop))
    }
  }
}
