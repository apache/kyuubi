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
import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.spark.sql.SparkSessionExtensions

import java.nio.file.Files

/**
 * Tests for RangerSparkExtensionSuite
 * on JdbcTableCatalog with DataSource V2 API.
 */
class IcebergTableCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "hive"

  val catalogV2 = "local"
  val jdbcCatalogV2 = "jdbc2"
  val namespace1 = "ns1"
  val namespace2 = "ns2"
  val table1 = "table1"
  val table2 = "table2"
  val outputTable1 = "outputTable1"
  val cacheTable1 = "cacheTable1"

  override protected val extension: SparkSessionExtensions => Unit = {
    new RangerSparkExtension
  }


  override def beforeAll(): Unit = {
    if (isSparkV31OrGreater) {
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2",
        "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalogV2.type", "hadoop")
      val string = Files.createTempDirectory("spark-warehouse-hive").toString
      spark.conf.set(s"spark.sql.catalog.$catalogV2.warehouse",
        string)


      super.beforeAll()

      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace1"))
      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1" +
          " (id int, name string, city string) USING iceberg"))

      doAs(
        "admin",
        sql(s"INSERT INTO $catalogV2.$namespace1.$table1" +
          " (id , name , city ) VALUES (1, 'liangbowen','Guangzhou')"))
      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$outputTable1" +
          " (id int, name string, city string) USING iceberg"))
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sessionState.catalog.reset()
    spark.sessionState.conf.clear()
  }



  test("[KYUUBI #3424] MERGE INTO") {
    assume(isSparkV31OrGreater)

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
        "someone",
        sql(mergeIntoSql)))
    assert(e1.getMessage.contains(s"does not have [select] privilege" +
      s" on [$namespace1/$table1/id]"))

    try {
      SparkRangerAdminPlugin.getRangerConf.setBoolean(
        s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
        true)
      val e2 = intercept[AccessControlException](
        doAs(
          "someone",
          sql(mergeIntoSql)))
      assert(e2.getMessage.contains(s"does not have" +
        s" [select] privilege" +
        s" on [$namespace1/$table1/id,$namespace1/table1/name,$namespace1/$table1/city]," +
        s" [update] privilege on [$namespace1/$outputTable1]"))
    } finally {
      SparkRangerAdminPlugin.getRangerConf.setBoolean(
        s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
        false)
    }
  }

  test("[KYUUBI #3424] UPDATE TABLE") {
    assume(isSparkV31OrGreater)

//    // UpdateTable
//    val e5 = intercept[AccessControlException](
//      doAs(
//        "someone",
//        sql(s"UPDATE $catalogV2.$namespace1.$table1 SET city='Hangzhou' " +
//          " WHERE id=1")))
//    assert(e5.getMessage.contains(s"does not have [update] privilege" +
//      s" on [$namespace1/$table1]"))

    doAs(
      "admin",
      sql(s"UPDATE $catalogV2.$namespace1.$table1 SET city='Hangzhou' " +
        " WHERE id=1"))
  }

  test("[KYUUBI #3424] DELETE FROM TABLE") {
    assume(isSparkV31OrGreater)

    // DeleteFromTable
    val e6 = intercept[AccessControlException](
      doAs("someone", sql(s"DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=1")))
    assert(e6.getMessage.contains(s"does not have [update] privilege" +
      s" on [$namespace1/$table1]"))
  }
}
