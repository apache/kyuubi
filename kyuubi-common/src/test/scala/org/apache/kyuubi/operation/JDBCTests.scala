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

import java.sql.{Date, SQLException, SQLTimeoutException, Timestamp}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchResultsReq, TOpenSessionReq, TStatusCode}

import org.apache.kyuubi.KYUUBI_VERSION

trait JDBCTests extends BasicJDBCTests {
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

  test("execute statement - select interval") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT interval '1' day AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === "1 days")
      assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.VARCHAR)
      val metaData = resultSet.getMetaData
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
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

  // TODO: https://github.com/apache/incubator-kyuubi/issues/937
  // Kyuubi docker image has not updated to lastest version
  ignore("kyuubi defined function - kyuubi_version") {
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

  // dockerfile use kyuubi as user which is not same with non-k8s env.
  ignore("kyuubi defined function - system_user") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT system_user()")
      assert(rs.next())
      assert(rs.getString(1) == System.getProperty("user.name"))
    }
  }
}
