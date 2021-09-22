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

import scala.collection.mutable.Set

import org.apache.kyuubi.sql.KyuubiSQLConf

class SqlClassificationSuite extends KyuubiSparkSQLExtensionTest {
  test("Sql classification for ddl") {
    withSQLConf(KyuubiSQLConf.SQL_CLASSIFICATION_ENABLED.key -> "true") {
      withDatabase("inventory") {
        val df = sql("CREATE DATABASE inventory;")
        assert(df.sparkSession.conf.get("kyuubi.spark.sql.classification") === "ddl")
      }
      val df = sql("select timestamp'2021-06-01'")
      assert(df.sparkSession.conf.get("kyuubi.spark.sql.classification") !== "ddl")
    }
  }

  test("Sql classification for dml") {
    withSQLConf(KyuubiSQLConf.SQL_CLASSIFICATION_ENABLED.key -> "true") {
      val df01 = sql("CREATE TABLE IF NOT EXISTS students " +
        "(name VARCHAR(64), address VARCHAR(64)) " +
        "USING PARQUET PARTITIONED BY (student_id INT);")
      assert(df01.sparkSession.conf.get("kyuubi.spark.sql.classification") === "ddl")

      val sql02 = "INSERT INTO students VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);"
      val df02 = sql(sql02)

      // scalastyle:off println
      println("the query execution is :" + spark.sessionState.executePlan(
        spark.sessionState.sqlParser.parsePlan(sql02)).toString())
      // scalastyle:on println

      assert(df02.sparkSession.conf.get("kyuubi.spark.sql.classification") === "dml")
    }
  }

  test("Sql classification for other and dql") {
    withSQLConf(KyuubiSQLConf.SQL_CLASSIFICATION_ENABLED.key -> "true") {
      val df01 = sql("SET spark.sql.variable.substitute=false")
      assert(df01.sparkSession.conf.get("kyuubi.spark.sql.classification") === "other")

      val sql02 = "select timestamp'2021-06-01'"
      val df02 = sql(sql02)

      assert(df02.sparkSession.conf.get("kyuubi.spark.sql.classification") === "dql")
    }
  }

  // TODO: #1064
  // TODO: The matching rule for sql classification should be generated automatically not manually
  test("get simple name for DDL") {

    import scala.collection.mutable.Set

    val ddlSimpleName: Set[String] = Set()

    // Notice: When we get Analyzed Logical Plan, the field of LogicalPlan.analyzed.analyzed is true

    // ALTER DATABASE
    val sql = "CREATE DATABASE inventory;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql)
      ).getClass.getSimpleName
    )
    val sql02 = "ALTER DATABASE inventory SET DBPROPERTIES " +
      "('Edited-by' = 'John', 'Edit-date' = '01/01/2001');"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql02)
      ).getClass.getSimpleName
    )

    // ALTER TABLE RENAME
    val sql03 = "CREATE TABLE student (name VARCHAR(64), rollno INT, age INT) " +
      "USING PARQUET PARTITIONED BY (age);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql03)
      ).getClass.getSimpleName
    )
    val sql04 = "INSERT INTO student VALUES " +
      "('zhang', 1, 10),('yu', 2, 11),('xiang', 3, 12),('zhangyuxiang', 4, 17);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql04)
      ).getClass.getSimpleName
    )
    val sql05 = "ALTER TABLE Student RENAME TO StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql05)
      ).getClass.getSimpleName
    )

    // ALTER TABLE RENAME PARTITION
    val sql06 = "ALTER TABLE default.StudentInfo PARTITION (age='10') " +
      "RENAME TO PARTITION (age='15');"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql06)
      ).getClass.getSimpleName
    )

    var pre_sql = "CREATE TABLE IF NOT EXISTS StudentInfo " +
      "(name VARCHAR(64), rollno INT, age INT) USING PARQUET PARTITIONED BY (age);"
    spark.sql(pre_sql)
    // ALTER TABLE ADD COLUMNS
    val sql07 = "ALTER TABLE StudentInfo ADD columns (LastName string, DOB timestamp);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql07)
      ).getClass.getSimpleName
    )

    // ALTER TABLE ALTER COLUMN
    val sql08 = "ALTER TABLE StudentInfo ALTER COLUMN age COMMENT \"new comment\";"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql08)
      ).getClass.getSimpleName
    )

    // ALTER TABLE CHANGE COLUMN
    val sql09 = "ALTER TABLE StudentInfo CHANGE COLUMN age COMMENT \"new comment123\";"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql09)
      ).getClass.getSimpleName
    )

    // ALTER TABLE ADD PARTITION
    val sql10 = "ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql10)
      ).getClass.getSimpleName
    )

    // ALTER TABLE DROP PARTITION
    val sql11 = "ALTER TABLE StudentInfo DROP IF EXISTS PARTITION (age=18);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql11)
      ).getClass.getSimpleName
    )

    // CREAT VIEW
    val sql12 = "CREATE OR REPLACE VIEW studentinfo_view " +
      "AS SELECT name, rollno FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql12)
      ).getClass.getSimpleName
    )

    // ALTER VIEW RENAME TO
    val sql13 = "ALTER VIEW studentinfo_view RENAME TO studentinfo_view2;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql13)
      ).getClass.getSimpleName
    )

    // ALTER VIEW SET TBLPROPERTIES
    val sql14 = "ALTER VIEW studentinfo_view2 SET TBLPROPERTIES " +
      "('created.by.user' = \"zhangyuxiang\", 'created.date' = '08-20-2021' );"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql14)
      ).getClass.getSimpleName
    )

    // ALTER VIEW UNSET TBLPROPERTIES
    val sql15 = "ALTER VIEW studentinfo_view2 UNSET TBLPROPERTIES " +
      "('created.by.user', 'created.date');"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql15)
      ).getClass.getSimpleName
    )

    // ALTER VIEW AS SELECT
    val sql16 = "ALTER VIEW studentinfo_view2 AS SELECT * FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql16)
      ).getClass.getSimpleName
    )

    // CREATE DATASOURCE TABLE AS SELECT
    val sql17 = "CREATE TABLE student_copy USING CSV AS SELECT * FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql17)
      ).getClass.getSimpleName
    )

    // CREATE DATASOURCE TABLE AS LIKE
    val sql18 = "CREATE TABLE Student_Dupli like studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql18)
      ).getClass.getSimpleName
    )

    // USE DATABASE
    val sql26 = "USE default;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql26)
      ).getClass.getSimpleName
    )

    // DROP DATABASE
    val sql19 = "DROP DATABASE IF EXISTS inventory_db CASCADE;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql19)
      ).getClass.getSimpleName
    )

    // CREATE FUNCTION
    val sql20 = "CREATE FUNCTION test_avg AS " +
      "'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql20)
      ).getClass.getSimpleName
    )

    // DROP FUNCTION
    val sql21 = "DROP FUNCTION test_avg;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql21)
      ).getClass.getSimpleName
    )

    spark.sql("CREATE TABLE IF NOT EXISTS studentabc (name VARCHAR(64), rollno INT, age INT) " +
      "USING PARQUET PARTITIONED BY (age);")
    // DROP TABLE
    val sql22 = "DROP TABLE IF EXISTS studentabc;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql22)
      ).getClass.getSimpleName
    )

    // DROP VIEW
    val sql23 = "DROP VIEW studentinfo_view2;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql23)
      ).getClass.getSimpleName
    )

    // TRUNCATE TABLE
    val sql24 = "TRUNCATE TABLE StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql24)
      ).getClass.getSimpleName
    )

    val sql27 = "TRUNCATE TABLE StudentInfo partition(age=10);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql27)
      ).getClass.getSimpleName
    )

    // REPAIR TABLE
    val sql25 = "MSCK REPAIR TABLE StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql25)
      ).getClass.getSimpleName
    )
    // scalastyle:off println
    println("ddl simple name is :" + ddlSimpleName.toSeq.sorted)
    // scalastyle:on println
  }

  // TODO: #1064
  // TODO: The matching rule for sql classification should be generated automatically not manually
  test("get simple name for DML") {
    import scala.collection.mutable.Set
    val dmlSimpleName: Set[String] = Set()

    var pre_sql = "CREATE TABLE IF NOT EXISTS students (name VARCHAR(64), address VARCHAR(64)) " +
      "USING PARQUET PARTITIONED BY (student_id INT);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS PERSONS (name VARCHAR(64), address VARCHAR(64)) " +
      "USING PARQUET PARTITIONED BY (ssn INT);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO persons VALUES " +
      "('Dora Williams', '134 Forest Ave, Menlo Park', 123456789), " +
      "('Eddie Davis', '245 Market St, Milpitas', 345678901);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS visiting_students " +
      "(name VARCHAR(64), address VARCHAR(64)) USING PARQUET PARTITIONED BY (student_id INT);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS applicants " +
      "(name VARCHAR(64), address VARCHAR(64), qualified BOOLEAN) " +
      "USING PARQUET PARTITIONED BY (student_id INT);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO applicants VALUES " +
      "('Helen Davis', '469 Mission St, San Diego', true, 999999), " +
      "('Ivy King', '367 Leigh Ave, Santa Clara', false, 101010), " +
      "('Jason Wang', '908 Bird St, Saratoga', true, 121212);"
    spark.sql(pre_sql)

    val sql01 = "INSERT INTO students VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql01)
      ).getClass.getSimpleName
    )

    val sql02 = "INSERT INTO students VALUES " +
      "('Bob Brown', '456 Taylor St, Cupertino', 222222), " +
      "('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql02)
      ).getClass.getSimpleName
    )

    val sql03 = "INSERT INTO students PARTITION (student_id = 444444) " +
      "SELECT name, address FROM persons WHERE name = \"Dora Williams\";"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql03)
      ).getClass.getSimpleName
    )

    val sql04 = "INSERT INTO students TABLE visiting_students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql04)
      ).getClass.getSimpleName
    )

    val sql05 = "INSERT INTO students FROM applicants " +
      "SELECT name, address, student_id WHERE qualified = true;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql05)
      ).getClass.getSimpleName
    )

    val sql06 = "INSERT INTO students (address, name, student_id) " +
      "VALUES ('Hangzhou, China', 'Kent Yao', 11215016);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql06)
      ).getClass.getSimpleName
    )

    val sql07 = "INSERT INTO students PARTITION (student_id = 11215017) " +
      "(address, name) VALUES ('Hangzhou, China', 'Kent Yao Jr.');"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql07)
      ).getClass.getSimpleName
    )

    val sql08 = "INSERT OVERWRITE students VALUES " +
      "('Ashua Hill', '456 Erica Ct, Cupertino', 111111), " +
      "('Brian Reed', '723 Kern Ave, Palo Alto', 222222);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql08)
      ).getClass.getSimpleName
    )

    val sql09 = "INSERT OVERWRITE students PARTITION (student_id = 222222) " +
      "SELECT name, address FROM persons WHERE name = \"Dora Williams\";"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql09)
      ).getClass.getSimpleName
    )

    val sql10 = "INSERT OVERWRITE students TABLE visiting_students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql10)
      ).getClass.getSimpleName
    )

    val sql11 = "INSERT OVERWRITE students FROM applicants " +
      "SELECT name, address, student_id WHERE qualified = true;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql11)
      ).getClass.getSimpleName
    )

    val sql12 = "INSERT OVERWRITE students (address, name, student_id) VALUES " +
      "('Hangzhou, China', 'Kent Yao', 11215016);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql12)
      ).getClass.getSimpleName
    )

    val sql13 = "INSERT OVERWRITE students PARTITION (student_id = 11215016) " +
      "(address, name) VALUES ('Hangzhou, China', 'Kent Yao Jr.');"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql13)
      ).getClass.getSimpleName
    )

    val sql14 = "INSERT OVERWRITE DIRECTORY '/tmp/destination' " +
      "USING parquet OPTIONS (col1 1, col2 2, col3 'test') " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql14)
      ).getClass.getSimpleName
    )

    val sql15 = "INSERT OVERWRITE DIRECTORY " +
      "USING parquet " +
      "OPTIONS ('path' '/tmp/destination', col1 1, col2 2, col3 'test') " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql15)
      ).getClass.getSimpleName
    )

    val sql016 = "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination' " +
      "STORED AS orc " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql016)
      ).getClass.getSimpleName
    )

    val sql017 = "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql017)
      ).getClass.getSimpleName
    )

    pre_sql = "CREATE TABLE IF NOT EXISTS students_test " +
      "(name VARCHAR(64), address VARCHAR(64)) " +
      "USING PARQUET PARTITIONED BY (student_id INT) " +
      "LOCATION '/tmp/destination/';"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO students_test VALUES " +
      "('Bob Brown', '456 Taylor St, Cupertino', 222222), " +
      "('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS test_load " +
      "(name VARCHAR(64), address VARCHAR(64), student_id INT) " +
      "USING HIVE;"
    spark.sql(pre_sql)

    val sql018 = "LOAD DATA LOCAL INPATH " +
      "'/tmp/destination/students_test' OVERWRITE INTO TABLE test_load;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql018)
      ).getClass.getSimpleName
    )

    pre_sql = "CREATE TABLE IF NOT EXISTS test_partition " +
      "(c1 INT, c2 INT, c3 INT) PARTITIONED BY (c2, c3) " +
      "LOCATION '/tmp/destination/';"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO test_partition PARTITION (c2 = 2, c3 = 3) VALUES (1);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO test_partition PARTITION (c2 = 5, c3 = 6) VALUES (4);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO test_partition PARTITION (c2 = 8, c3 = 9) VALUES (7);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS test_load_partition " +
      "(c1 INT, c2 INT, c3 INT) USING HIVE PARTITIONED BY (c2, c3);"
    spark.sql(pre_sql)

    val sql019 = "LOAD DATA LOCAL INPATH '/tmp/destination/test_partition/c2=2/c3=3' " +
      "OVERWRITE INTO TABLE test_load_partition PARTITION (c2=2, c3=3);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql019)
      ).getClass.getSimpleName
    )
    // scalastyle:off println
    println("dml simple name is :" + dmlSimpleName)
    // scalastyle:on println
  }

  // TODO: #1064
  // TODO: The matching rule for sql classification should be generated automatically not manually
  test("get simple name for auxiliary statement") {
    val auxiStatementSimpleName: Set[String] = Set()

    var pre_sql = "CREATE TABLE IF NOT EXISTS students_testtest " +
      "(name STRING, student_id INT) PARTITIONED BY (student_id);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO students_testtest PARTITION (student_id = 111111) VALUES ('Mark');"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO students_testtest PARTITION (student_id = 222222) VALUES ('John');"
    spark.sql(pre_sql)

    val sql01 = "ANALYZE TABLE students_testtest COMPUTE STATISTICS NOSCAN;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql01)
      ).getClass.getSimpleName
    )

    val sql48 = "ANALYZE TABLE students_testtest PARTITION " +
      "(student_id = 111111) COMPUTE STATISTICS;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql48)
      ).getClass.getSimpleName
    )

    val sql49 = "ANALYZE TABLE students_testtest COMPUTE STATISTICS FOR COLUMNS name;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql49)
      ).getClass.getSimpleName
    )

    val sql29 = "SHOW PARTITIONS students_testtest;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql29)
      ).getClass.getSimpleName
    )

    val sql30 = "SHOW PARTITIONS students_testtest PARTITION (student_id = 111111);"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql30)
      ).getClass.getSimpleName
    )

    val sql31 = "SHOW TABLE EXTENDED LIKE 'students_testtest';"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql31)
      ).getClass.getSimpleName
    )

    val sql32 = "SHOW TABLES;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql32)
      ).getClass.getSimpleName
    )

    val sql33 = "SHOW TABLES FROM default;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql33)
      ).getClass.getSimpleName
    )

    val sql02 = "CACHE TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') " +
      "SELECT * FROM students_testtest;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql02)
      ).getClass.getSimpleName
    )

    val sql03 = "CACHE LAZY TABLE testCache OPTIONS ('storageLevel' 'DISK_ONLY') " +
      "SELECT * FROM students_testtest;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql03)
      ).getClass.getSimpleName
    )

    val sql09 = "REFRESH \"hdfs://path/to/table\""
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql09)
      ).getClass.getSimpleName
    )

    val sql04 = "UNCACHE TABLE IF EXISTS testCache;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql04)
      ).getClass.getSimpleName
    )

    val sql05 = "CLEAR CACHE;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql05)
      ).getClass.getSimpleName
    )

    val sql06 = "REFRESH TABLE students_testtest;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql06)
      ).getClass.getSimpleName
    )

    pre_sql = "CREATE OR REPLACE VIEW students_testtest_view " +
      "AS SELECT name, student_id FROM students_testtest;"
    spark.sql(pre_sql)
    val sql07 = "REFRESH TABLE default.students_testtest_view;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql07)
      ).getClass.getSimpleName
    )

    val sql35 = "SHOW VIEWS;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql35)
      ).getClass.getSimpleName
    )

    pre_sql = "DROP VIEW IF EXISTS students_testtest_view;"
    spark.sql(pre_sql)

    pre_sql = "CREATE FUNCTION IF NOT EXISTS test_avg AS " +
      "'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';"
    spark.sql(pre_sql)
    val sql08 = "REFRESH FUNCTION test_avg;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql08)
      ).getClass.getSimpleName
    )

    val sql15 = "DESC FUNCTION test_avg;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql15)
      ).getClass.getSimpleName
    )

    val sql26 = "SHOW FUNCTIONS test_avg;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql26)
      ).getClass.getSimpleName
    )

    val sql27 = "SHOW SYSTEM FUNCTIONS concat;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql27)
      ).getClass.getSimpleName
    )

    val sql28 = "SHOW FUNCTIONS LIKE 'test*';"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql28)
      ).getClass.getSimpleName
    )

    pre_sql = "DROP FUNCTION IF EXISTS test_avg;"
    spark.sql(pre_sql)

    pre_sql = "CREATE DATABASE IF NOT EXISTS employees COMMENT 'For software companies';"
    spark.sql(pre_sql)
    val sql10 = "DESCRIBE DATABASE employees;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql10)
      ).getClass.getSimpleName
    )

    val sql23 = "SHOW DATABASES;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql23)
      ).getClass.getSimpleName
    )

    val sql24 = "SHOW DATABASES LIKE 'employ*';"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql24)
      ).getClass.getSimpleName
    )

    val sql25 = "SHOW SCHEMAS;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql25)
      ).getClass.getSimpleName
    )

    pre_sql = "ALTER DATABASE employees SET DBPROPERTIES " +
      "('Create-by' = 'Kevin', 'Create-date' = '09/01/2019');"
    val sql11 = "DESCRIBE DATABASE EXTENDED employees;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql11)
      ).getClass.getSimpleName
    )
    pre_sql = "DROP DATABASE IF EXISTS employees;"
    spark.sql(pre_sql)

    pre_sql = "CREATE TABLE IF NOT EXISTS customer" +
      "(cust_id INT, state VARCHAR(20), name STRING COMMENT 'Short name') " +
      "USING parquet PARTITIONED BY (state);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO customer PARTITION (state = 'AR') VALUES (100, 'Mike');"
    val sql12 = "DESCRIBE TABLE customer;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql12)
      ).getClass.getSimpleName
    )

    val sql21 = "SHOW COLUMNS IN customer;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql21)
      ).getClass.getSimpleName
    )

    val sql22 = "SHOW CREATE TABLE customer;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql22)
      ).getClass.getSimpleName
    )

    val sql13 = "DESCRIBE TABLE EXTENDED customer PARTITION (state = 'AR');"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql13)
      ).getClass.getSimpleName
    )

    val sql14 = "DESCRIBE customer salesdb.customer.name;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql14)
      ).getClass.getSimpleName
    )
    pre_sql = "DROP TABLE IF EXISTS customer;"
    spark.sql(pre_sql)

    pre_sql = "CREATE TABLE IF NOT EXISTS person " +
      "(name STRING , age INT COMMENT 'Age column', address STRING);"
    spark.sql(pre_sql)
    val sql16 = "DESCRIBE QUERY SELECT age, sum(age) FROM person GROUP BY age;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql16)
      ).getClass.getSimpleName
    )

    val sql17 = "DESCRIBE QUERY WITH all_names_cte AS " +
      "(SELECT name from person) SELECT * FROM all_names_cte;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql17)
      ).getClass.getSimpleName
    )

    val sql18 = "DESC QUERY VALUES(100, 'John', 10000.20D) " +
      "AS employee(id, name, salary);"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql18)
      ).getClass.getSimpleName
    )

    val sql19 = "DESC QUERY TABLE person;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql19)
      ).getClass.getSimpleName
    )

    val sql20 = "DESCRIBE FROM person SELECT age;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql20)
      ).getClass.getSimpleName
    )
    pre_sql = "DROP TABLE IF EXISTS person;"
    spark.sql(pre_sql)

    pre_sql = "CREATE TABLE IF NOT EXISTS customer" +
      "(cust_code INT, name VARCHAR(100), cust_addr STRING) " +
      "TBLPROPERTIES ('created.by.user' = 'John', 'created.date' = '01-01-2001');"
    spark.sql(pre_sql)
    val sql34 = "SHOW TBLPROPERTIES customer;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql34)
      ).getClass.getSimpleName
    )

    val sql36 = "SET spark.sql.variable.substitute=false"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql36)
      ).getClass.getSimpleName
    )

    val sql37 = "SET"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql37)
      ).getClass.getSimpleName
    )

    val sql38 = "SET spark.sql.variable.substitute"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql38)
      ).getClass.getSimpleName
    )

    val sql39 = "RESET"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql39)
      ).getClass.getSimpleName
    )

    val sql40 = "RESET spark.sql.variable.substitute"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql40)
      ).getClass.getSimpleName
    )

    val sql41 = "SET TIME ZONE LOCAL;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql41)
      ).getClass.getSimpleName
    )

    val sql42 = "ADD FILE /tmp/test;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql42)
      ).getClass.getSimpleName
    )

    val sql43 = "ADD JAR /tmp/test.jar;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql43)
      ).getClass.getSimpleName
    )

    val sql44 = "LIST FILE;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql44)
      ).getClass.getSimpleName
    )

    val sql45 = "LIST JAR;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql45)
      ).getClass.getSimpleName
    )

    val sql46 = "EXPLAIN select k, sum(v) from values (1, 2), (1, 3) t(k, v) group by k;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql46)
      ).getClass.getSimpleName
    )

    val sql47 = "EXPLAIN EXTENDED select k, sum(v) from values (1, 2), (1, 3) t(k, v) group by k;"
    auxiStatementSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql47)
      ).getClass.getSimpleName
    )

    // scalastyle:off println
    println("auxiliary statement simple name is :" + auxiStatementSimpleName.toSeq.sorted)
    // scalastyle:on println
  }
}
