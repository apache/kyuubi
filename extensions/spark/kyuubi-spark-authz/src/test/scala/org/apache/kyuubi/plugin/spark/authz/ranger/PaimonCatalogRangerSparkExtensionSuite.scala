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

import org.scalatest.Outcome

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.RangerTestUsers._
import org.apache.kyuubi.tags.PaimonTest
import org.apache.kyuubi.util.AssertionUtils._

/**
 * Tests for RangerSparkExtensionSuite on Paimon
 */
@PaimonTest
class PaimonCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"
  private def isSupportedVersion = true

  val catalogV2 = "paimon_catalog"
  val namespace1 = "paimon_ns"
  val table1 = "table1"

  override def withFixture(test: NoArgTest): Outcome = {
    assume(isSupportedVersion)
    test()
  }

  override def beforeAll(): Unit = {
    if (isSupportedVersion) {
      spark.conf.set(s"spark.sql.catalog.$catalogV2", "org.apache.paimon.spark.SparkCatalog")
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2.warehouse",
        Utils.createTempDir(catalogV2).toString)
      super.beforeAll()
    }

    doAs(admin, sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace1"))
  }

  override def afterAll(): Unit = {
    if (isSupportedVersion) {
      doAs(admin, sql(s"DROP DATABASE IF EXISTS $catalogV2.$namespace1"))

      super.afterAll()
      spark.sessionState.catalog.reset()
      spark.sessionState.conf.clear()
    }
  }

  test("CreateTable") {
    withCleanTmpResources(Seq((s"$catalogV2.$namespace1.$table1", "table"))) {
      val createTable =
        s"""
           |CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1
           |(id int, name string, city string)
           |USING paimon
           |OPTIONS (
           | primaryKey = 'id'
           |)
           |""".stripMargin

      interceptEndsWith[AccessControlException] {
        doAs(someone, sql(createTable))
      }(s"does not have [create] privilege on [$namespace1/$table1]")
      doAs(admin, createTable)
    }
  }
}
