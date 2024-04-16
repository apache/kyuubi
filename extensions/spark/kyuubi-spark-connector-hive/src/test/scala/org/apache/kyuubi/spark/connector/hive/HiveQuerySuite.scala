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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

class HiveQuerySuite extends KyuubiHiveTest {

  def withTempNonPartitionedTable(
      spark: SparkSession,
      table: String,
      format: String = "PARQUET",
      hiveTable: Boolean = false)(f: => Unit): Unit = {
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS
         | $table (id String, date String)
         | ${if (hiveTable) "STORED AS" else "USING"} $format
         |""".stripMargin).collect()
    try f
    finally spark.sql(s"DROP TABLE $table")
  }

  def withTempPartitionedTable(
      spark: SparkSession,
      table: String,
      format: String = "PARQUET",
      hiveTable: Boolean = false)(f: => Unit): Unit = {
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS
         | $table (id String, year String, month string)
         | ${if (hiveTable) "STORED AS" else "USING"} $format
         | PARTITIONED BY (year, month)
         |""".stripMargin).collect()
    try f
    finally spark.sql(s"DROP TABLE $table")
  }

  def checkQueryResult(
      sql: String,
      sparkSession: SparkSession,
      excepted: Array[Row]): Unit = {
    val result = sparkSession.sql(sql).collect()
    assert(result sameElements excepted)
  }

  test("simple query") {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempNonPartitionedTable(spark, table) {
        // can query an existing Hive table in three sections
        val result = spark.sql(
          s"""
             | SELECT * FROM $table
             |""".stripMargin)
        assert(result.collect().isEmpty)

        // error msg should contains catalog info if table is not exist
        val e = intercept[AnalysisException] {
          spark.sql(
            s"""
               | SELECT * FROM hive.ns1.tb1
               |""".stripMargin)
        }
        assert(e.message.contains(
          "[TABLE_OR_VIEW_NOT_FOUND] The table or view `hive`.`ns1`.`tb1` cannot be found.") ||
          e.message.contains("Table or view not found: hive.ns1.tb1"))
      }
    }
  }

  test("Non partitioned table insert") {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempNonPartitionedTable(spark, table) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table
             | VALUES("yi", "2022-08-08")
             |""".stripMargin).collect()

        checkQueryResult(s"select * from $table", spark, Array(Row.apply("yi", "2022-08-08")))
      }
    }
  }

  test("Partitioned table insert and all dynamic insert") {
    withSparkSession(Map("hive.exec.dynamic.partition.mode" -> "nonstrict")) { spark =>
      val table = "hive.default.employee"
      withTempPartitionedTable(spark, table) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table
             | VALUES("yi", "2022", "0808")
             |""".stripMargin).collect()

        checkQueryResult(s"select * from $table", spark, Array(Row.apply("yi", "2022", "0808")))
      }
    }
  }

  test("[KYUUBI #4525] Partitioning predicates should take effect to filter data") {
    withSparkSession(Map("hive.exec.dynamic.partition.mode" -> "nonstrict")) { spark =>
      val table = "hive.default.employee"
      withTempPartitionedTable(spark, table) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table
             | VALUES("yi", "2022", "0808"),("yi", "2023", "0316")
             |""".stripMargin).collect()

        checkQueryResult(
          s"select * from $table where year = '2022'",
          spark,
          Array(Row.apply("yi", "2022", "0808")))

        checkQueryResult(
          s"select * from $table where year = '2023'",
          spark,
          Array(Row.apply("yi", "2023", "0316")))
      }
    }
  }

  test("Partitioned table insert and all static insert") {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempPartitionedTable(spark, table) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table PARTITION(year = '2022', month = '08')
             | VALUES("yi")
             |""".stripMargin).collect()

        checkQueryResult(s"select * from $table", spark, Array(Row.apply("yi", "2022", "08")))
      }
    }
  }

  test("Partitioned table insert overwrite static and dynamic insert") {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempPartitionedTable(spark, table) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table PARTITION(year = '2022')
             | VALUES("yi", "08")
             |""".stripMargin).collect()

        checkQueryResult(s"select * from $table", spark, Array(Row.apply("yi", "2022", "08")))
      }
    }
  }

  test("[KYUUBI #5414] Reader should not polluted the global hiveconf instance") {
    withSparkSession() { spark =>
      val table = "hive.default.hiveconf_test"
      withTempPartitionedTable(spark, table, "ORC", hiveTable = true) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table PARTITION(year = '2022')
             | VALUES("yi", "08")
             |""".stripMargin).collect()

        checkQueryResult(s"select * from $table", spark, Array(Row.apply("yi", "2022", "08")))
        checkQueryResult(s"select count(*) as c from $table", spark, Array(Row.apply(1)))
      }
    }
  }

  test("Partitioned table insert and static partition value is empty string") {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempPartitionedTable(spark, table) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table PARTITION(year = '', month = '08')
             | VALUES("yi")
             |""".stripMargin).collect()

        checkQueryResult(s"select * from $table", spark, Array(Row.apply("yi", null, "08")))
      }
    }
  }

  test("read partitioned avro table") {
    readPartitionedTable("AVRO", true)
    readPartitionedTable("AVRO", false)
  }

  test("read un-partitioned avro table") {
    readUnPartitionedTable("AVRO", true)
    readUnPartitionedTable("AVRO", false)
  }

  test("read partitioned textfile table") {
    readPartitionedTable("TEXTFILE", true)
    readPartitionedTable("TEXTFILE", false)
  }

  test("read un-partitioned textfile table") {
    readUnPartitionedTable("TEXTFILE", true)
    readUnPartitionedTable("TEXTFILE", false)
  }

  test("read partitioned SequenceFile table") {
    readPartitionedTable("SequenceFile", true)
    readPartitionedTable("SequenceFile", false)
  }

  test("read un-partitioned SequenceFile table") {
    readUnPartitionedTable("SequenceFile", true)
    readUnPartitionedTable("SequenceFile", false)
  }

  test("read partitioned ORC table") {
    readPartitionedTable("ORC", true)
    readPartitionedTable("ORC", false)
  }

  test("read un-partitioned ORC table") {
    readUnPartitionedTable("ORC", true)
    readUnPartitionedTable("ORC", false)
  }

  test("Partitioned table insert into static and dynamic insert") {
    val table = "hive.default.employee"
    withTempPartitionedTable(spark, table) {
      spark.sql(
        s"""
           | INSERT INTO
           | $table PARTITION(year = '2022')
           | SELECT * FROM VALUES("yi", "08")
           |""".stripMargin).collect()

      checkQueryResult(s"select * from $table", spark, Array(Row.apply("yi", "2022", "08")))
    }
  }

  private def readPartitionedTable(format: String, hiveTable: Boolean): Unit = {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempPartitionedTable(spark, table, format, hiveTable) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table PARTITION(year = '2023')
             | VALUES("zhao", "09")
             |""".stripMargin)
        checkQueryResult(s"select * from $table", spark, Array(Row.apply("zhao", "2023", "09")))
      }
    }
  }

  private def readUnPartitionedTable(format: String, hiveTable: Boolean): Unit = {
    withSparkSession() { spark =>
      val table = "hive.default.employee"
      withTempNonPartitionedTable(spark, table, format, hiveTable) {
        spark.sql(
          s"""
             | INSERT OVERWRITE
             | $table
             | VALUES("zhao", "2023-09-21")
             |""".stripMargin).collect()

        checkQueryResult(s"select * from $table", spark, Array(Row.apply("zhao", "2023-09-21")))
      }
    }
  }
}
