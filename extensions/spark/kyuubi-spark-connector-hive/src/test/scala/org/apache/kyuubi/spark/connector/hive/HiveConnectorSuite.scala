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

package org.apache.kyuubi.spark.connector.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class HiveConnectorSuite extends KyuubiFunSuite {

  def withTempTable(spark: SparkSession, table: String)(f: => Unit): Unit = {
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS
         | $table (id String, date String)
         | USING PARQUET
         | PARTITIONED BY (date)
         |""".stripMargin).collect()
    try f
    finally spark.sql(s"DROP TABLE $table")
  }

  test("simple query") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.catalog.hivev2", classOf[HiveTableCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val table = "default.employee"
      withTempTable(spark, table) {
        spark.sql(
          s"""
             | INSERT INTO
             | $table
             | VALUES("yi", "2022-08-08")
             |""".stripMargin).collect()

        // can query an existing Hive table in three sections
        val result = spark.sql(
          s"""
             | SELECT * FROM hivev2.$table
             |""".stripMargin)
        assert(result.collect().head == Row("yi", "2022-08-08"))

        // error msg should contains catalog info if table is not exist
        val e = intercept[AnalysisException] {
          spark.sql(
            s"""
               | SELECT * FROM hivev2.ns1.tb1
               |""".stripMargin)
        }
        assert(e.getMessage().contains("Table or view not found: hivev2.ns1.tb1"))
      }
    }
  }
}
