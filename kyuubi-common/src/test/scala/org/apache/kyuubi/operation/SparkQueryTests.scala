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

package org.apache.kyuubi.operation

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.sql.{Date, SQLException, SQLTimeoutException, Timestamp}
import java.util.jar.{JarEntry, JarOutputStream}

import scala.collection.JavaConverters._
import scala.tools.nsc._

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchResultsReq, TOpenSessionReq, TStatusCode}

import org.apache.kyuubi.{KYUUBI_VERSION, Utils}

trait SparkQueryTests extends HiveJDBCTestHelper {

  protected lazy val SPARK_ENGINE_MAJOR_MINOR_VERSION: (Int, Int) = sparkEngineMajorMinorVersion

  test("execute statement - select null") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT NULL AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === null)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.NULL)
      assert(metaData.getPrecision(1) === 0)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select boolean") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT false AS col")
      assert(resultSet.next())
      assert(!resultSet.getBoolean("col"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BOOLEAN)
      assert(metaData.getPrecision(1) === 1)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select tinyint") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 1Y AS col")
      assert(resultSet.next())
      assert(resultSet.getByte("col") === 1.toByte)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TINYINT)
      assert(metaData.getPrecision(1) === 3)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select smallint") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 1S AS col")
      assert(resultSet.next())
      assert(resultSet.getShort("col") === 1.toShort)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.SMALLINT)
      assert(metaData.getPrecision(1) === 5)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select int") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 4 AS col")
      assert(resultSet.next())
      assert(resultSet.getInt("col") === 4)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.INTEGER)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select long") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 4L AS col")
      assert(resultSet.next())
      assert(resultSet.getLong("col") === 4L)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BIGINT)
      assert(metaData.getPrecision(1) === 19)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select float") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast(1.2 as float) AS col")
      assert(resultSet.next())
      assert(resultSet.getFloat("col") === 1.2f)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.FLOAT)
      assert(metaData.getPrecision(1) === 7)
      assert(metaData.getScale(1) === 7)
    }
  }

  test("execute statement - select double") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 4.2D AS col")
      assert(resultSet.next())
      assert(resultSet.getDouble("col") === 4.2d)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DOUBLE)
      assert(metaData.getPrecision(1) === 15)
      assert(metaData.getScale(1) === 15)
    }
  }

  test("execute statement - select string") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 'kentyao' AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === "kentyao")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select binary") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast('kyuubi' as binary) AS col")
      assert(resultSet.next())
      assert(resultSet.getObject("col") === "kyuubi".getBytes)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BINARY)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select date") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT DATE '2018-11-17' AS col")
      assert(resultSet.next())
      assert(resultSet.getDate("col") === Date.valueOf("2018-11-17"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DATE)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select timestamp") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT TIMESTAMP '2018-11-17 13:33:33' AS col")
      assert(resultSet.next())
      assert(resultSet.getTimestamp("col") === Timestamp.valueOf("2018-11-17 13:33:33"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TIMESTAMP)
      assert(metaData.getPrecision(1) === 29)
      assert(metaData.getScale(1) === 9)
    }
  }

  test("execute statement - select timestamp_ntz") {
    assume(SPARK_ENGINE_MAJOR_MINOR_VERSION._1 > 3 ||
      (SPARK_ENGINE_MAJOR_MINOR_VERSION._1 == 3 && SPARK_ENGINE_MAJOR_MINOR_VERSION._2 >= 4))
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery(
        "SELECT make_timestamp_ntz(2022, 03, 24, 18, 08, 31.800) AS col")
      assert(resultSet.next())
      assert(resultSet.getTimestamp("col") === Timestamp.valueOf("2022-03-24 18:08:31.800"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TIMESTAMP)
      assert(metaData.getPrecision(1) === 29)
      assert(metaData.getScale(1) === 9)
    }
  }

  test("execute statement - select daytime interval") {
    withJdbcStatement() { statement =>
      Map(
        "interval 1 day 1 hour -60 minutes 30 seconds" ->
          Tuple2("1 00:00:30.000000000", "1 days 30 seconds"),
        "interval 30 seconds 12345 milliseconds" ->
          Tuple2("0 00:00:42.345000000", "42.345 seconds"),
        "interval 1 hour 59 minutes 30 seconds 12345 milliseconds" ->
          Tuple2("0 01:59:42.345000000", "1 hours 59 minutes 42.345 seconds"),
        "-interval 2 day" -> Tuple2("-2 00:00:00.000000000", "-2 days"),
        "interval 59 minutes 30 seconds 12345 milliseconds" ->
          Tuple2("0 00:59:42.345000000", "59 minutes 42.345 seconds"),
        "interval 25 hour" -> Tuple2("1 01:00:00.000000000", "25 hours"),
        "interval 1 hour 62 minutes" -> Tuple2("0 02:02:00.000000000", "2 hours 2 minutes"),
        "interval 1 day 1 hour 59 minutes 30 seconds 12345 milliseconds" ->
          Tuple2("1 01:59:42.345000000", "1 days 1 hours 59 minutes 42.345 seconds"),
        "interval 1 day 1 hour -60 minutes" -> Tuple2("1 00:00:00.000000000", "1 days"),
        "INTERVAL 30 SECONDS" -> Tuple2("0 00:00:30.000000000", "30 seconds"),
        "interval -60 minutes 30 seconds" ->
          Tuple2("-0 00:59:30.000000000", "-59 minutes -30 seconds"),
        "-interval 200 day" -> Tuple2("-200 00:00:00.000000000", "-200 days"),
        "interval 1 hour -60 minutes 30 seconds" -> Tuple2("0 00:00:30.000000000", "30 seconds"),
        "interval 62 minutes" -> Tuple2("0 01:02:00.000000000", "1 hours 2 minutes"),
        "interval 1 day 1 hour" -> Tuple2("1 01:00:00.000000000", "1 days 1 hours")).foreach {
        kv => // value -> result pair
          val resultSet = statement.executeQuery(s"SELECT ${kv._1} AS col")
          assert(resultSet.next())
          val result = resultSet.getString("col")
          val metaData = resultSet.getMetaData
          if (SPARK_ENGINE_MAJOR_MINOR_VERSION._1 == 3
            && SPARK_ENGINE_MAJOR_MINOR_VERSION._2 < 2) {
            // for spark 3.1 and backwards
            assert(result === kv._2._2)
            assert(metaData.getPrecision(1) === Int.MaxValue)
            assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.VARCHAR)
          } else {
            assert(result === kv._2._1)
            assert(metaData.getPrecision(1) === 29)
            assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.OTHER)
          }
          assert(metaData.getScale(1) === 0)
      }
    }
  }

  test("execute statement - select year/month interval") {
    withJdbcStatement() { statement =>
      Map(
        "INTERVAL 2022 YEAR" -> Tuple2("2022-0", "2022 years"),
        "INTERVAL '2021-07' YEAR TO MONTH" -> Tuple2("2021-7", "2021 years 7 months"),
        "INTERVAL 3 MONTH" -> Tuple2("0-3", "3 months"),
        "INTERVAL 241 MONTH" -> Tuple2("20-1", "20 years 1 months"),
        "INTERVAL -1 year -25 MONTH" -> Tuple2("-3-1", "-3 years -1 months"),
        "INTERVAL 3 year -25 MONTH" -> Tuple2("0-11", "11 months")).foreach { kv =>
        val resultSet = statement.executeQuery(s"SELECT ${kv._1} AS col")
        assert(resultSet.next())
        val result = resultSet.getString("col")
        val metaData = resultSet.getMetaData
        if (SPARK_ENGINE_MAJOR_MINOR_VERSION._1 == 3
          && SPARK_ENGINE_MAJOR_MINOR_VERSION._2 < 2) {
          // for spark 3.1 and backwards
          assert(result === kv._2._2)
          assert(metaData.getPrecision(1) === Int.MaxValue)
          assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.VARCHAR)
        } else {
          assert(result === kv._2._1)
          assert(metaData.getPrecision(1) === 11)
          assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.OTHER)
        }
        assert(metaData.getScale(1) === 0)
      }
    }
  }

  test("execute statement - select array") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery(
        "SELECT array() AS col1, array(1) AS col2, array(null) AS col3")
      assert(resultSet.next())
      assert(resultSet.getObject("col1") === "[]")
      assert(resultSet.getObject("col2") === "[1]")
      assert(resultSet.getObject("col3") === "[null]")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.ARRAY)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getPrecision(2) == Int.MaxValue)
      assert(metaData.getScale(1) == 0)
      assert(metaData.getScale(2) == 0)
    }
  }

  test("execute statement - select map") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery(
        "SELECT map() AS col1, map(1, 2, 3, 4) AS col2, map(1, null) AS col3")
      assert(resultSet.next())
      assert(resultSet.getObject("col1") === "{}")
      assert(resultSet.getObject("col2") === "{1:2,3:4}")
      assert(resultSet.getObject("col3") === "{1:null}")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.JAVA_OBJECT)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getPrecision(2) == Int.MaxValue)
      assert(metaData.getScale(1) == 0)
      assert(metaData.getScale(2) == 0)
    }
  }

  test("execute statement - select struct") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery(
        "SELECT struct('1', '2') AS col1," +
          " named_struct('a', 2, 'b', 4) AS col2," +
          " named_struct('a', null, 'b', null) AS col3")
      assert(resultSet.next())
      assert(resultSet.getObject("col1") === """{"col1":"1","col2":"2"}""")
      assert(resultSet.getObject("col2") === """{"a":2,"b":4}""")
      assert(resultSet.getObject("col3") === """{"a":null,"b":null}""")

      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.STRUCT)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getPrecision(2) == Int.MaxValue)
      assert(metaData.getScale(1) == 0)
      assert(metaData.getScale(2) == 0)
    }
  }

  test("execute statement - analysis exception") {
    val sql = "select date_sub(date'2011-11-11', '1.2')"

    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery(sql)
      }
      assert(e.getMessage
        .contains("The second argument of 'date_sub' function needs to be an integer."))
    }
  }

  test("execute statement - select with variable substitution") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("chengpan")
      req.setPassword("123")
      val conf = Map(
        "use:database" -> "default",
        "set:hiveconf:a" -> "x",
        "set:hivevar:b" -> "y",
        "set:metaconf:c" -> "z",
        "set:system:s" -> "s")
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val status = tOpenSessionResp.getStatus
      assert(status.getStatusCode === TStatusCode.SUCCESS_STATUS)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      // hive matched behaviors
      tExecuteStatementReq.setStatement(
        """
          |select
          | '${hiveconf:a}' as col_0,
          | '${hivevar:b}'  as col_1,
          | '${b}'          as col_2
          |""".stripMargin)
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(1)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(tFetchResultsResp.getResults.getColumns.get(0).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp.getResults.getColumns.get(1).getStringVal.getValues.get(0) === "y")
      assert(tFetchResultsResp.getResults.getColumns.get(2).getStringVal.getValues.get(0) === "y")

      val tExecuteStatementReq2 = new TExecuteStatementReq()
      tExecuteStatementReq2.setSessionHandle(tOpenSessionResp.getSessionHandle)
      // spark specific behaviors
      tExecuteStatementReq2.setStatement(
        """
          |select
          | '${a}'             as col_0,
          | '${hivevar:a}'     as col_1,
          | '${spark:a}'       as col_2,
          | '${sparkconf:a}'   as col_3,
          | '${not_exist_var}' as col_4,
          | '${c}'             as col_5,
          | '${s}'             as col_6
          |""".stripMargin)
      val tExecuteStatementResp2 = client.ExecuteStatement(tExecuteStatementReq2)
      val tFetchResultsReq2 = new TFetchResultsReq()
      tFetchResultsReq2.setOperationHandle(tExecuteStatementResp2.getOperationHandle)
      tFetchResultsReq2.setFetchType(0)
      tFetchResultsReq2.setMaxRows(1)
      val tFetchResultsResp2 = client.FetchResults(tFetchResultsReq2)
      assert(tFetchResultsResp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(tFetchResultsResp2.getResults.getColumns.get(0).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp2.getResults.getColumns.get(1).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp2.getResults.getColumns.get(2).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp2.getResults.getColumns.get(3).getStringVal.getValues.get(0) === "x")
      // for not exist vars, hive return "${not_exist_var}" itself, but spark return ""
      assert(tFetchResultsResp2.getResults.getColumns.get(4).getStringVal.getValues.get(0) === "")

      assert(tFetchResultsResp2.getResults.getColumns.get(5).getStringVal.getValues.get(0) === "z")
      assert(tFetchResultsResp2.getResults.getColumns.get(6).getStringVal.getValues.get(0) === "s")
    }
  }

  test("execute statement - select with builtin functions") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT substring('kentyao', 1)")
      assert(resultSet.next())
      assert(resultSet.getString("substring(kentyao, 1, 2147483647)") === "kentyao")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("query time out shall respect client-side if no server-side control") {
    withJdbcStatement() { statement =>
      statement.setQueryTimeout(1)
      val e = intercept[SQLTimeoutException] {
        statement.execute("select java_method('java.lang.Thread', 'sleep', 10000L)")
      }.getMessage
      assert(e.contains("Query timed out after"))

      statement.setQueryTimeout(0)
      val rs1 = statement.executeQuery(
        "select 'test', java_method('java.lang.Thread', 'sleep', 3000L)")
      rs1.next()
      assert(rs1.getString(1) == "test")

      statement.setQueryTimeout(-1)
      val rs2 = statement.executeQuery(
        "select 'test', java_method('java.lang.Thread', 'sleep', 3000L)")
      rs2.next()
      assert(rs2.getString(1) == "test")
    }
  }

  test("kyuubi defined function - kyuubi_version") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT kyuubi_version()")
      assert(rs.next())
      assert(rs.getString(1) == KYUUBI_VERSION)
    }
  }

  test("kyuubi defined function - engine_name") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT engine_name()")
      assert(rs.next())
      assert(StringUtils.isNotBlank(rs.getString(1)))
    }
  }

  test("kyuubi defined function - engine_id") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT engine_id()")
      assert(rs.next())
      assert(StringUtils.isNotBlank(rs.getString(1)))
    }
  }

  test("KYUUBI #1059: Plan only operations") {
    val ddl = "create table t(a int) using parquet"
    val dql = "select * from t"
    val setkey = "SET kyuubi.operation.plan.only.mode"
    withJdbcStatement("t") { statement =>
      try {
        statement.execute("SET kyuubi.operation.plan.only.mode=optimize")
        val set = statement.executeQuery(ddl)
        assert(set.next())
        assert(set.getString("plan") startsWith "Create")
        val set0 = statement.executeQuery(setkey)
        assert(set0.next())
        assert(set0.getString(2) === "optimize")
        val e1 = intercept[SQLException](statement.executeQuery(dql))
        assert(e1.getMessage.contains("Table or view not found"))
        statement.execute("SET kyuubi.operation.plan.only.mode=analyze")
        val e2 = intercept[SQLException](statement.executeQuery(dql))
        assert(e2.getMessage.contains("Table or view not found"))
        statement.execute("SET kyuubi.operation.plan.only.mode=parse")
        val set1 = statement.executeQuery(dql)
        assert(set1.next())
        assert(set1.getString("plan") contains "Unresolved")
      } finally {
        statement.executeQuery("SET kyuubi.operation.plan.only.mode=none")
        statement.executeQuery(ddl)
        val res = statement.executeQuery(dql)
        assert(!res.next(), "table t exists in none mode")
      }
    }
  }

  test("execute simple scala code") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val rs = statement.executeQuery("spark.version")
      rs.next()
      // scala repl will return resX = YYYYY, and here we only check YYYYY
      val sparkVer = rs.getString(1).split("=")(1).trim
      assert("\\d\\.\\d\\.\\d(-SNAPSHOT)?".r.pattern.matcher(sparkVer).matches())
      assert(rs.getMetaData.getColumnName(1) === "output")
    }
  }

  test("execute simple scala code with result returned") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val code =
        """
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .toDF
          |""".stripMargin
      val rs1 = statement.executeQuery(code)
      rs1.next()
      assert(rs1.getString(1) startsWith "df: org.apache.spark.sql.DataFrame")

      // continue
      val rs2 = statement.executeQuery("df.count()")
      rs2.next()
      assert(rs2.getString(1).endsWith("5"))

      // continue
      val rs3 = statement.executeQuery("result.set(df)")
      for (i <- Range(0, 10, 2)) {
        assert(rs3.next)
        assert(rs3.getInt(1) === i)
      }

      // switch to sql
      val set =
        """
          |spark.conf.set("kyuubi.operation.language", "SQL")
          |""".stripMargin
      val t = statement.executeQuery(set)
      t.next()
      val rs4 = statement.executeQuery("select 12345")
      assert(rs4.next())
      assert(rs4.getInt(1) === 12345)

      // switch to scala again
      statement.execute("SET kyuubi.operation.language=scala")
      val code2 =
        """
          |/* this
          | * is
          | * a
          | * multi-line comments
          | */
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .map(x => (x, x + 1, x * 2)) // this is a single-line comment
          |  .toDF
          |""".stripMargin
      val rs5 = statement.executeQuery(code2)
      rs5.next()
      assert(rs5.getString(1) startsWith "df: org.apache.spark.sql.DataFrame")

      // re-assign
      val rs6 = statement.executeQuery("result.set(df)")
      for (i <- Range(0, 10, 2)) {
        assert(rs6.next)
        assert(rs6.getInt(2) === i + 1)
      }
    }
  }

  test("incomplete scala code block will fail") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      // incomplete code block
      val incompleteCode =
        """
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .map {
          |    x => (x, x + 1, x * 2)
          |""".stripMargin
      val e = intercept[SQLException](statement.executeQuery(incompleteCode))
      assert(e.getMessage contains "Incomplete code:")
    }
  }

  test("scala code compile error will fail") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      // incomplete code block
      val incompleteCode =
        """
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .map { x => (x, x + 1, y * 2) } // y is missing
          |""".stripMargin
      val e = intercept[SQLException](statement.executeQuery(incompleteCode))
      assert(e.getMessage contains "not found: value y")
    }
  }

  test("scala code with console output") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val code = """spark.sql("SET kyuubi.operation.language").show(false)"""
      val rs = statement.executeQuery(code)

      var foundOperationLangItem = false
      while (rs.next() && !foundOperationLangItem) {
        foundOperationLangItem = rs.getString(1).contains("kyuubi.operation.language")
      }
      assert(foundOperationLangItem)
    }
  }

  test("scala code with loading external package at runtime ") {
    val jarDir = Utils.createTempDir().toFile

    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val udfCode =
        """
          |package test.utils
          |
          |object Math {
          |def add(x: Int, y: Int): Int = x + y
          |
          |def main(args: Array[String]): Unit = {
          |println(add(1, 2))
          |}
          |}
          |
          |""".stripMargin

      val jarFile = createJarFile(udfCode, "test-function.jar", jarDir.toString)
      val jarPath = jarFile.getAbsolutePath
      val code0 = """spark.sql("SET kyuubi.operation.language").show(false)"""
      val code1 = s"""spark.sql("add jar ${jarPath}")"""
      val code2 = """val x = test.utils.Math.add(1,2)"""
      statement.execute(code0)
      statement.execute(code1)
      val rs = statement.executeQuery(code2)
      rs.next()
      assert(rs.getString(1) == "x: Int = 3")
    }
  }

  def createJarFile(codeText: String, jarName: String, outputDir: String): File = {
    val codeFile = new File(outputDir, "test.scala")
    FileUtils.writeStringToFile(codeFile, codeText, "UTF-8")

    val settings = new Settings
    settings.outputDirs.setSingleOutput(outputDir)
    settings.usejavacp.value = true
    val global = new Global(settings)
    val runner = new global.Run
    runner.compile(List(codeFile.getAbsolutePath))
    val jarFile = new File(outputDir, jarName)
    val targetJar = new JarOutputStream(new FileOutputStream(jarFile))
    add(outputDir + "/test", targetJar, outputDir + "/")
    targetJar.close
    jarFile
  }

  private def add(folder: String, target: JarOutputStream, replacement: String): Unit = {
    val source = new File(folder)
    if (source.isDirectory) {
      for (nestedFile <- source.listFiles) {
        add(nestedFile.getAbsolutePath, target, replacement)
      }
    } else {
      val entry = new JarEntry(source.getPath
        .replace("\\", "/")
        .replace(replacement, ""))
      entry.setTime(source.lastModified)
      target.putNextEntry(entry)
      val byteArray = Files.readAllBytes(source.toPath)
      target.write(byteArray)
      target.closeEntry()
    }
  }

  def sparkEngineMajorMinorVersion: (Int, Int) = {
    var sparkRuntimeVer = ""
    withJdbcStatement() { stmt =>
      val result = stmt.executeQuery("SELECT version()")
      assert(result.next())
      sparkRuntimeVer = result.getString(1)
      assert(!result.next())
    }
    Utils.majorMinorVersion(sparkRuntimeVer)
  }
}
