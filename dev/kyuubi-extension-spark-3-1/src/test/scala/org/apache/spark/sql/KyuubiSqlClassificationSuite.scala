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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.test.SQLTestUtils

import org.apache.kyuubi.sql.KyuubiSQLConf

class KyuubiSqlClassificationSuite extends QueryTest
    with SQLTestUtils with AdaptiveSparkPlanHelper {

  var _spark: SparkSession = _
  override def spark: SparkSession = _spark

  protected override def beforeAll(): Unit = {
    _spark = SparkSession.builder()
      .master("local[1]")
      .config(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLExtension")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.hadoop.hive.metastore.client.capability.check", "false")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()
    setupData()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    cleanupData()
    if (_spark != null) {
      _spark.stop()
    }
  }

  private def setupData(): Unit = {
    val self = _spark
    import self.implicits._
    spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString)), 10)
      .toDF("c1", "c2").createOrReplaceTempView("t1")
    spark.sparkContext.parallelize(
      (1 to 10).map(i => TestData(i, i.toString)), 5)
      .toDF("c1", "c2").createOrReplaceTempView("t2")
    spark.sparkContext.parallelize(
      (1 to 50).map(i => TestData(i, i.toString)), 2)
      .toDF("c1", "c2").createOrReplaceTempView("t3")
  }

  private def cleanupData(): Unit = {
    spark.sql("DROP VIEW IF EXISTS t1")
    spark.sql("DROP VIEW IF EXISTS t2")
    spark.sql("DROP VIEW IF EXISTS t3")
  }

  test("get simple name for DDL") {

    import scala.collection.mutable.Set

    val ddlSimpleName: Set[String] = Set()

    // ALTER DATABASE
    val sql = "CREATE DATABASE inventory;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql).getClass.getSimpleName
    )
    val sql02 = "ALTER DATABASE inventory SET DBPROPERTIES " +
      "('Edited-by' = 'John', 'Edit-date' = '01/01/2001');"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql02).getClass.getSimpleName
    )

    // ALTER TABLE RENAME
    val sql03 = "CREATE TABLE student (name VARCHAR(64), rollno INT, age INT) " +
      "USING PARQUET PARTITIONED BY (age);"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql03).getClass.getSimpleName
    )
    val sql04 = "INSERT INTO student VALUES " +
      "('zhang', 1, 10),('yu', 2, 11),('xiang', 3, 12),('zhangyuxiang', 4, 17);"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql04).getClass.getSimpleName
    )
    val sql05 = "ALTER TABLE Student RENAME TO StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql05).getClass.getSimpleName
    )

    // ALTER TABLE RENAME PARTITION
    val sql06 = "ALTER TABLE default.StudentInfo PARTITION (age='10') " +
      "RENAME TO PARTITION (age='15');"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql06).getClass.getSimpleName
    )

    // ALTER TABLE ADD COLUMNS
    val sql07 = "ALTER TABLE StudentInfo ADD columns (LastName string, DOB timestamp);"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql07).getClass.getSimpleName
    )

    // ALTER TABLE ALTER COLUMN
    val sql08 = "ALTER TABLE StudentInfo ALTER COLUMN LastName COMMENT \"new comment\";"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql08).getClass.getSimpleName
    )

    // ALTER TABLE CHANGE COLUMN
    val sql09 = "ALTER TABLE StudentInfo CHANGE COLUMN LastName COMMENT \"new comment123\";"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql09).getClass.getSimpleName
    )

    // ALTER TABLE ADD PARTITION
    val sql10 = "ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18);"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql10).getClass.getSimpleName
    )

    // ALTER TABLE DROP PARTITION
    val sql11 = "ALTER TABLE StudentInfo DROP IF EXISTS PARTITION (age=18);"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql11).getClass.getSimpleName
    )

    // CREAT VIEW
    val sql12 = "CREATE OR REPLACE VIEW studentinfo_view " +
      "AS SELECT name, rollno FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql12).getClass.getSimpleName
    )

    // ALTER VIEW RENAME TO
    val sql13 = "ALTER VIEW studentinfo_view RENAME TO studentinfo_view2;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql13).getClass.getSimpleName
    )

    // ALTER VIEW SET TBLPROPERTIES
    val sql14 = "ALTER VIEW studentinfo_view2 SET TBLPROPERTIES " +
      "('created.by.user' = \"zhangyuxiang\", 'created.date' = '08-20-2021' );"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql14).getClass.getSimpleName
    )

    // ALTER VIEW UNSET TBLPROPERTIES
    val sql15 = "ALTER VIEW studentinfo_view2 UNSET TBLPROPERTIES " +
      "('created.by.user', 'created.date');"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql15).getClass.getSimpleName
    )

    // ALTER VIEW AS SELECT
    val sql16 = "ALTER VIEW studentinfo_view2 AS SELECT * FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql16).getClass.getSimpleName
    )

    // CREATE DATASOURCE TABLE AS SELECT
    val sql17 = "CREATE TABLE student_copy USING CSV AS SELECT * FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql17).getClass.getSimpleName
    )

    // CREATE DATASOURCE TABLE AS SELECT
    val sql18 = "CREATE TABLE Student_Dupli like studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql18).getClass.getSimpleName
    )

    // USE DATABASE
    val sql26 = "USE default;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql26).getClass.getSimpleName
    )

    // DROP DATABASE
    val sql19 = "DROP DATABASE IF EXISTS inventory_db CASCADE;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql19).getClass.getSimpleName
    )

    // CREATE FUNCTION
    val sql20 = "CREATE FUNCTION test_avg AS " +
      "'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql20).getClass.getSimpleName
    )

    // DROP FUNCTION
    val sql21 = "DROP FUNCTION test_avg;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql21).getClass.getSimpleName
    )

    // DROP TABLE
    val sql22 = "DROP TABLE IF EXISTS student_copy;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql22).getClass.getSimpleName
    )

    // DROP VIEW
    val sql23 = "DROP VIEW studentinfo_view2;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql23).getClass.getSimpleName
    )

    // TRUNCATE TABLE
    val sql24 = "TRUNCATE TABLE StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql24).getClass.getSimpleName
    )

    // REPAIR TABLE
    val sql25 = "MSCK REPAIR TABLE StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.sqlParser.parsePlan(sql25).getClass.getSimpleName
    )
    // scalastyle:off println
    println("ddl simple name is :" + ddlSimpleName)
    // scalastyle:on println
  }

  test("Sql classification for ddl") {
    withSQLConf(KyuubiSQLConf.SQL_CLASSIFICATION_ENABLED.key -> "true") {
      withDatabase("inventory") {
        val df = sql("CREATE DATABASE inventory;")
        assert(df.sparkSession.conf.get("spark.sql.classification") === "ddl")
      }
      val df = sql("select timestamp'2021-06-01'")
      assert(df.sparkSession.conf.get("spark.sql.classification") !== "ddl")
    }
  }
}
