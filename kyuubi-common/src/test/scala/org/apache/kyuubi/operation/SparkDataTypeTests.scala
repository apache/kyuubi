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

import java.sql.{Date, Timestamp}

import org.apache.kyuubi.engine.SemanticVersion

trait SparkDataTypeTests extends HiveJDBCTestHelper {
  protected lazy val SPARK_ENGINE_VERSION = sparkEngineMajorMinorVersion

  def resultCodec: String = "simple"

  test("execute statement - select null") {
    assume(resultCodec == "simple" || (resultCodec == "arrow" && SPARK_ENGINE_VERSION >= "3.2"))
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
    assume(SPARK_ENGINE_VERSION >= "3.4")
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
    assume(resultCodec == "simple" || (resultCodec == "arrow" && SPARK_ENGINE_VERSION >= "3.3"))
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
          if (SPARK_ENGINE_VERSION < "3.2") {
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
    assume(resultCodec == "simple" || (resultCodec == "arrow" && SPARK_ENGINE_VERSION >= "3.3"))
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
        if (SPARK_ENGINE_VERSION < "3.2") {
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
    assume(resultCodec == "simple" || (resultCodec == "arrow" && SPARK_ENGINE_VERSION >= "3.2"))
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
    assume(resultCodec == "simple" || (resultCodec == "arrow" && SPARK_ENGINE_VERSION >= "3.2"))
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
    assume(resultCodec == "simple" || (resultCodec == "arrow" && SPARK_ENGINE_VERSION >= "3.2"))
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

  def sparkEngineMajorMinorVersion: SemanticVersion = {
    var sparkRuntimeVer = ""
    withJdbcStatement() { stmt =>
      val result = stmt.executeQuery("SELECT version()")
      assert(result.next())
      sparkRuntimeVer = result.getString(1)
      assert(!result.next())
    }
    SemanticVersion(sparkRuntimeVer)
  }
}
