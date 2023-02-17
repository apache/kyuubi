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

package org.apache.kyuubi.engine.spark.operation

import java.sql.Statement

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.SparkDataTypeTests

class SparkArrowbasedOperationSuite extends WithSparkSQLEngine with SparkDataTypeTests {

  override protected def jdbcUrl: String = getJdbcUrl

  override def withKyuubiConf: Map[String, String] = Map.empty

  override def jdbcVars: Map[String, String] = {
    Map(KyuubiConf.OPERATION_RESULT_FORMAT.key -> resultFormat)
  }

  override def resultFormat: String = "arrow"

  override def beforeEach(): Unit = {
    super.beforeEach()
    withJdbcStatement() { statement =>
      checkResultSetFormat(statement, "arrow")
    }
  }

  test("detect resultSet format") {
    withJdbcStatement() { statement =>
      checkResultSetFormat(statement, "arrow")
      statement.executeQuery(s"set ${KyuubiConf.OPERATION_RESULT_FORMAT.key}=thrift")
      checkResultSetFormat(statement, "thrift")
    }
  }

  test("Spark session timezone format") {
    withJdbcStatement() { statement =>
      def check(expect: String): Unit = {
        val query =
          """
            |SELECT
            |  from_utc_timestamp(
            |    from_unixtime(
            |      1670404535000 / 1000, 'yyyy-MM-dd HH:mm:ss'
            |    ),
            |    'GMT+08:00'
            |  )
            |""".stripMargin
        val resultSet = statement.executeQuery(query)
        assert(resultSet.next())
        assert(resultSet.getString(1) == expect)
      }

      def setTimeZone(timeZone: String): Unit = {
        val rs = statement.executeQuery(s"set spark.sql.session.timeZone=$timeZone")
        assert(rs.next())
      }

      Seq("true", "false").foreach { timestampAsString =>
        statement.executeQuery(
          s"set ${KyuubiConf.ARROW_BASED_ROWSET_TIMESTAMP_AS_STRING.key}=$timestampAsString")
        checkArrowBasedRowSetTimestampAsString(statement, timestampAsString)
        setTimeZone("UTC")
        check("2022-12-07 17:15:35.0")
        setTimeZone("GMT+8")
        check("2022-12-08 01:15:35.0")
      }
    }
  }

  private def checkResultSetFormat(statement: Statement, expectFormat: String): Unit = {
    val query =
      s"""
         |SELECT '$${hivevar:${KyuubiConf.OPERATION_RESULT_FORMAT.key}}' AS col
         |""".stripMargin
    val resultSet = statement.executeQuery(query)
    assert(resultSet.next())
    assert(resultSet.getString("col") === expectFormat)
  }

  private def checkArrowBasedRowSetTimestampAsString(
      statement: Statement,
      expect: String): Unit = {
    val query =
      s"""
         |SELECT '$${hivevar:${KyuubiConf.ARROW_BASED_ROWSET_TIMESTAMP_AS_STRING.key}}' AS col
         |""".stripMargin
    val resultSet = statement.executeQuery(query)
    assert(resultSet.next())
    assert(resultSet.getString("col") === expect)
  }
}
