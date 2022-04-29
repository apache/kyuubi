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

package org.apache.kyuubi.engine.trino

import java.sql.Date
import java.sql.Timestamp

import org.apache.kyuubi.operation.HiveJDBCTestHelper

trait TrinoQueryTests extends HiveJDBCTestHelper {

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
      val resultSet = statement.executeQuery("SELECT TINYINT '1' AS col")
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
      val resultSet = statement.executeQuery("SELECT SMALLINT '1' AS col")
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
      val resultSet = statement.executeQuery("SELECT INTEGER '4' AS col")
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
      val resultSet = statement.executeQuery("SELECT BIGINT '4' AS col")
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
      val resultSet = statement.executeQuery("SELECT REAL '1.2' AS col")
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
      val resultSet = statement.executeQuery("SELECT DOUBLE '4.2' AS col")
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
      val resultSet = statement.executeQuery("SELECT 'hongdd' AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === "hongdd")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select binary") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT VARBINARY 'hongdd' AS col")
      assert(resultSet.next())
      assert(resultSet.getObject("col") === "hongdd".getBytes)
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

  test("execute statement - select daytime interval") {
    withJdbcStatement() { statement =>
      Map(
        "interval '1' day + interval '1' hour - interval '60' minute + interval '30' second" ->
          "1 00:00:30.000000000",
        "interval '1' day + interval '1' hour - interval '60' minute" ->
          "1 00:00:00.000000000",
        "interval '30' second" -> "0 00:00:30.000000000",
        "interval '-60' minute + interval '30' second" -> "-0 00:59:30.000000000",
        "-interval '200' day" -> "-200 00:00:00.000000000",
        "interval '1' hour - interval '60' minute + interval '30' second" -> "0 00:00:30.000000000",
        "interval '62' minute" -> "0 01:02:00.000000000",
        "interval '1' day + interval '1' hour" -> "1 01:00:00.000000000").foreach {
        kv => // value -> result pair
          val resultSet = statement.executeQuery(s"SELECT ${kv._1} AS col")
          assert(resultSet.next())
          val result = resultSet.getString("col")
          val metaData = resultSet.getMetaData
          assert(result === kv._2)
          assert(metaData.getPrecision(1) === 29)
          assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.OTHER)
          assert(metaData.getScale(1) === 0)
      }
    }
  }

  test("execute statement - select year/month interval") {
    withJdbcStatement() { statement =>
      Map(
        "INTERVAL '2022' YEAR" -> "2022-0",
        "INTERVAL '2021-07' YEAR TO MONTH" -> "2021-7",
        "INTERVAL '3' MONTH" -> "0-3",
        "INTERVAL '241' MONTH" -> "20-1",
        "INTERVAL '-1' year + INTERVAL '-25' MONTH" -> "-3-1",
        "INTERVAL '3' year + INTERVAL '-25' MONTH" -> "0-11").foreach { kv =>
        val resultSet = statement.executeQuery(s"SELECT ${kv._1} AS col")
        assert(resultSet.next())
        val result = resultSet.getString("col")
        val metaData = resultSet.getMetaData
        assert(result === kv._2)
        assert(metaData.getPrecision(1) === 11)
        assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.OTHER)
        assert(metaData.getScale(1) === 0)
      }
    }
  }

  test("execute statement - select array") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery(
        "SELECT ARRAY[] AS col1, ARRAY[1] AS col2, ARRAY[null] AS col3")
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
        "SELECT MAP() AS col1, MAP(ARRAY[1, 3], ARRAY[2, 4]) AS col2, " +
          "map(ARRAY[1], ARRAY[null]) AS col3")
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
        "SELECT ROW('1', '2') as col1," +
          " cast(ROW(2, 4) as ROW(a INT, b INT)) as col2," +
          " cast(ROW(NULL, NULL) as ROW(a INT, b INT)) as col3")
      assert(resultSet.next())
      assert(resultSet.getObject("col1") === """{"1","2"}""")
      assert(resultSet.getObject("col2") === """{a=2,b=4}""")
      assert(resultSet.getObject("col3") === """{a=null,b=null}""")

      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.STRUCT)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getPrecision(2) == Int.MaxValue)
      assert(metaData.getScale(1) == 0)
      assert(metaData.getScale(2) == 0)
    }
  }
}
