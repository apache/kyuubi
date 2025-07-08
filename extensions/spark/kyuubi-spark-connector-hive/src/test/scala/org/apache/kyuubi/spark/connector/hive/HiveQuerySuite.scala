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

  test("ORC filter pushdown") {
    val table = "hive.default.orc_filter_pushdown"
    withTable(table) {
      spark.sql(
        s"""
           | CREATE TABLE $table (
           |  id INT,
           |  data STRING,
           |  value INT
           |  ) PARTITIONED BY (dt STRING, region STRING)
           |  STORED AS ORC
           | """.stripMargin).collect()

      // Insert test data with partitions
      spark.sql(
        s"""
           | INSERT INTO $table PARTITION (dt='2024-01-01', region='east')
           | VALUES (1, 'a', 100), (2, 'b', 200), (11, 'aa', 100), (22, 'b', 200)
           |""".stripMargin)

      spark.sql(
        s"""
           | INSERT INTO $table PARTITION (dt='2024-01-01', region='west')
           | VALUES (3, 'c', 300), (4, 'd', 400), (33, 'cc', 300), (44, 'dd', 400)
           |""".stripMargin)
      spark.sql(
        s"""
           | INSERT INTO $table PARTITION (dt='2024-01-02', region='east')
           | VALUES (5, 'e', 500), (6, 'f', 600), (55, 'ee', 500), (66, 'ff', 600)
           | """.stripMargin)

      // Test multiple partition filters
      val df1 = spark.sql(
        s"""
           | SELECT * FROM $table
           | WHERE dt = '2024-01-01' AND region = 'east' AND value > 1500
           |""".stripMargin)
      assert(df1.count() === 0)

      // Test multiple partition filters
      val df2 = spark.sql(
        s"""
           | SELECT * FROM $table
           | WHERE dt = '2024-01-01' AND region = 'east' AND value > 150
           |""".stripMargin)
      assert(df2.count() === 2)
      assert(df2.collect().map(_.getInt(0)).toSet === Set(2, 22))

      // Test explain
      val df3 = spark.sql(
        s"""
           | EXPLAIN SELECT count(*) as total_rows
           | FROM $table
           | WHERE dt = '2024-01-01' AND region = 'east' AND value > 1
           |""".stripMargin)
      assert(df3.count() === 1)
      // contains like : PushedFilters: [IsNotNull(value), GreaterThan(value,1)]
      assert(df3.collect().map(_.getString(0)).filter { s =>
        s.contains("PushedFilters") && !s.contains("PushedFilters: []")
      }.toSet.size == 1)

      // Test aggregation pushdown partition filters
      spark.conf.set("spark.sql.orc.aggregatePushdown", true)

      // Test aggregation pushdown partition filters
      val df4 = spark.sql(
        s"""
           | SELECT count(*) as total_rows
           | FROM $table
           | WHERE dt = '2024-01-01' AND region = 'east'
           | group by dt, region
           | """.stripMargin)
      assert(df4.count() === 1)
      assert(df4.collect().map(_.getLong(0)).toSet === Set(4L))

      val df5 = spark.sql(
        s"""
           | EXPLAIN SELECT count(*) as total_rows
           | FROM $table
           | WHERE dt = '2024-01-01' AND region = 'east'
           | group by dt, region
           | """.stripMargin)
      assert(df5.count() === 1)
      // contains like :  PushedAggregation: [COUNT(*)],
      assert(df5.collect().map(_.getString(0)).filter { s =>
        s.contains("PushedAggregation") && !s.contains("PushedAggregation: []")
      }.toSet.size == 1)

      spark.conf.set("spark.sql.orc.aggregatePushdown", false)

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
