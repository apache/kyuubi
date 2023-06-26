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
package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.RebalancePartitions
import org.apache.spark.util.Utils

import org.apache.kyuubi.sql.KyuubiSQLConf

class IcebergRepartitionBeforeWritingSuite extends KyuubiSparkSQLExtensionTest {
  private val catalog = "catalog2"
  private val storage = "USING ICEBERG"

  override def sparkConf(): SparkConf = {
    val conf = super.sparkConf()
    val value =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
        "org.apache.kyuubi.sql.KyuubiSparkSQLExtension"
    Map(
      "spark.sql.catalogImplementation" -> "in-memory",
      "spark.sql.defaultCatalog" -> catalog,
      "spark.sql.extensions" -> value,
      "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
      "spark.sql.catalog.spark_catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> Utils.createTempDir().toPath.toString,
      s"spark.jars" -> System.getProperty("java.class.path")
        .split(":")
        .filter(_.contains("iceberg-spark")).head)
      .foreach(e => conf.set(e._1, e._2))
    conf
  }

  test("test INSERT OVERWRITE partitioned iceberg table") {
    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true") {
      withTable("tmp1", "tmp2") {
        sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
        sql(s"CREATE TABLE tmp2 (c1 int) $storage PARTITIONED BY (c2 string)")
        check(sql("INSERT OVERWRITE TABLE tmp1 " +
          "SELECT * FROM tmp2 WHERE c2='1'"))
      }

      withTable("tmp1", "tmp2") {
        sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
        sql(s"CREATE TABLE tmp2 (c1 int) $storage PARTITIONED BY (c2 string)")
        check(
          sql(
            """FROM VALUES(1),(2)
              |INSERT OVERWRITE TABLE tmp1 PARTITION(c2='a') SELECT *
              |INSERT OVERWRITE TABLE tmp2 PARTITION(c2='a') SELECT *
              |""".stripMargin),
          2)
      }

      withTable("tmp1") {
        sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
        check(
          sql("INSERT INTO TABLE tmp1 SELECT * FROM VALUES(1,'a'),(2,'b'),(3,'c') AS t(c1,c2)"))
      }

    }
  }

  test("test INSERT INTO non-partitioned iceberg table") {
    withTable("tmp1") {
      sql(s"CREATE TABLE tmp1 (c1 int) $storage")
      check(
        sql("INSERT INTO TABLE tmp1 SELECT * FROM VALUES(1),(2),(3) AS t(c1)"))
    }

  }

  test("test INSERT OVERWRITE non-partitioned iceberg table") {
    withTable("tmp1") {
      sql(s"CREATE TABLE tmp1 (c1 int) $storage")
      check(
        sql("INSERT OVERWRITE TABLE tmp1 SELECT * FROM VALUES(1),(2),(3) AS t(c1)"))
    }

    withTable("tmp1", "tmp2") {
      sql(s"CREATE TABLE tmp1 (c1 int) $storage")
      sql(s"CREATE TABLE tmp2 (c1 int) $storage")
      check(
        sql(
          """FROM VALUES(1),(2),(3)
            |INSERT OVERWRITE TABLE tmp1 SELECT *
            |INSERT OVERWRITE TABLE tmp2 SELECT *
            |""".stripMargin),
        2)
    }
  }

  private def check(df: => DataFrame, expectedRebalanceNum: Int = 1): Unit = {
    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "true") {
      assert(
        df.queryExecution.analyzed.collect {
          case r: RebalancePartitions => r
        }.size == expectedRebalanceNum)
    }
    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "false") {
      assert(
        df.queryExecution.analyzed.collect {
          case r: RebalancePartitions => r
        }.isEmpty)
    }
  }
}
