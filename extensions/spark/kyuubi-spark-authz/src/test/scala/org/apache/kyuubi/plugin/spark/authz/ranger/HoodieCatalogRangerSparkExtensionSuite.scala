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

import org.apache.spark.SparkConf
import org.scalatest.Outcome

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.RangerTestNamespace._
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.tags.HoodieTest
import org.apache.kyuubi.util.AssertionUtils.interceptContains

/**
 * Tests for RangerSparkExtensionSuite on Hoodie SQL.
 */
@HoodieTest
class HoodieCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
  override protected val sqlExtensions: String =
    if (isSparkV31OrGreater && !isSparkV35OrGreater) {
      "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    } else {
      ""
    }

  override protected val extraSparkConf: SparkConf =
    new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val namespace1 = hoodieNamespace
  val table1 = "table1_hoodie"
  val table2 = "table2_hoodie"
  val outputTable1 = "outputTable_hoodie"

  override def withFixture(test: NoArgTest): Outcome = {
    assume(isSparkV31OrGreater && !isSparkV35OrGreater)
    test()
  }

  override def beforeAll(): Unit = {
    if (isSparkV31OrGreater && !isSparkV35OrGreater) {
      if (isSparkV32OrGreater) {
        spark.conf.set(
          s"spark.sql.catalog.$sparkCatalog",
          "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        spark.conf.set(s"spark.sql.catalog.$sparkCatalog.type", "hadoop")
        spark.conf.set(
          s"spark.sql.catalog.$sparkCatalog.warehouse",
          Utils.createTempDir("hoodie-hadoop").toString)
      }
      super.beforeAll()
    }
  }

  override def afterAll(): Unit = {
    if (isSparkV31OrGreater && !isSparkV35OrGreater) {
      super.afterAll()
      spark.sessionState.catalog.reset()
      spark.sessionState.conf.clear()
    }
  }

  test("[KYUUBI #5284] Kyuubi authz support Hoodie Alter Table Command") {
    withCleanTmpResources(Seq((s"$namespace1.$table1", "table"), (namespace1, "database"))) {
      doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $namespace1"))
      doAs(
        admin,
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS $namespace1.$table1(id int, name string, city string)
             |USING hudi
             |OPTIONS (
             | type = 'cow',
             | primaryKey = 'id',
             | 'hoodie.datasource.hive_sync.enable' = 'false'
             |)
             |PARTITIONED BY(city)
             |""".stripMargin))

      interceptContains[AccessControlException](
        doAs(someone,
          sql(s"ALTER TABLE $namespace1.$table1 ADD COLUMNS(age int)")))(
        s"does not have [alter] privilege on [$namespace1/$table1/age]")

      interceptContains[AccessControlException](
        doAs(someone,
          sql(s"ALTER TABLE $namespace1.$table1 CHANGE COLUMN id id bigint")))(
        s"does not have [alter] privilege" +
          s" on [$namespace1/$table1/id]")

      interceptContains[AccessControlException](
        doAs(someone,
          sql(s"ALTER TABLE $namespace1.$table1 DROP PARTITION (city='test')")))(
        s"does not have [alter] privilege" +
          s" on [$namespace1/$table1/city]")

      interceptContains[AccessControlException](
        doAs(someone,
          sql(s"ALTER TABLE $namespace1.$table1 RENAME TO $namespace1.$table2")))(
        s"does not have [alter] privilege" +
          s" on [$namespace1/$table1]")
    }
  }
}
