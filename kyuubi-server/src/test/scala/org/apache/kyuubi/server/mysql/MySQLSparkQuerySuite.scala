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

package org.apache.kyuubi.server.mysql

import java.sql._

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{KYUUBI_VERSION, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols

class MySQLSparkQuerySuite extends WithKyuubiServer with MySQLJDBCTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()

  override protected val frontendProtocols: Seq[KyuubiConf.FrontendProtocols.Value]
  = FrontendProtocols.MYSQL :: Nil

  override protected def getJdbcUrl: String =
    s"jdbc:mysql://${server.frontendServices.head.connectionUrl}/"

  override protected def jdbcUrl: String = getJdbcUrl

  test("execute statement - select null") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT NULL AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === null)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.NULL)
      assert(metaData.getPrecision(1) === 25)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select boolean") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT false AS col")
      assert(resultSet.next())
      assert(!resultSet.getBoolean("col"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TINYINT)
      assert(metaData.getPrecision(1) === 3)
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
      assert(metaData.getColumnType(1) === java.sql.Types.REAL)
      assert(metaData.getPrecision(1) === 100)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select double") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 4.2D AS col")
      assert(resultSet.next())
      assert(resultSet.getDouble("col") === 4.2d)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DOUBLE)
      assert(metaData.getPrecision(1) === 100)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select string") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 'kentyao' AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === "kentyao")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === 25)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement - select binary") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast('kyuubi' as binary) AS col")
      assert(resultSet.next())
      assert(resultSet.getObject("col") === "kyuubi")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.CHAR)
      assert(metaData.getPrecision(1) === 25)
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
      assert(metaData.getPrecision(1) === 25)
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
      assert(metaData.getPrecision(1) === 25)
      assert(metaData.getScale(1) === 0)
    }
  }

  ignore("execute statement - select interval") {
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
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === 25)
      assert(metaData.getPrecision(2) == 25)
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
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === 25)
      assert(metaData.getPrecision(2) == 25)
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
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === 25)
      assert(metaData.getPrecision(2) == 25)
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

  test("execute statement - select with builtin functions") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT substring('kentyao', 1)")
      assert(resultSet.next())
      assert(resultSet.getString("substring(kentyao, 1, 2147483647)") === "kentyao")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === 25)
      assert(metaData.getScale(1) === 0)
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
}
