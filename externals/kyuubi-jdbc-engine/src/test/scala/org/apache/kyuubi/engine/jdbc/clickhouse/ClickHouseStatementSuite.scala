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
package org.apache.kyuubi.engine.jdbc.clickhouse

import java.sql.{Date, Timestamp}

import org.apache.kyuubi.operation.HiveJDBCTestHelper

class ClickHouseStatementSuite extends WithClickHouseEngine with HiveJDBCTestHelper {

  test("clickhouse - test select") {
    withJdbcStatement("test1") { statement =>
      statement.execute("create database if not exists db1")
      statement.execute("use db1")
      statement.execute(
        """CREATE TABLE db1.test1(id bigint, name varchar(255), age int) ENGINE=File(TabSeparated)
          |""".stripMargin)
      statement.execute("insert into db1.test1 values(1, 'a', 11)")

      val resultSet1 = statement.executeQuery("select * from db1.test1")
      while (resultSet1.next()) {
        val id = resultSet1.getObject(1)
        assert(id == 1)
        val name = resultSet1.getObject(2)
        assert(name == "a")
        val age = resultSet1.getObject(3)
        assert(age == 11)
      }
    }
  }

  test("clickhouse - test types") {
    withJdbcStatement("test1") { statement =>
      statement.execute("create database if not exists db1")
      statement.execute("use db1")
      statement.execute(
        """ CREATE TABLE db1.type_test(
          | id bigint,
          | tiny_col tinyint,
          | smallint_col smallint,
          | int_col int,
          | bigint_col bigint,
          | largeint_col bigint,
          | decimal_col decimal(27, 9),
          | date_col date,
          | datetime_col datetime,
          | char_col char,
          | varchar_col varchar(255),
          | string_col String,
          | boolean_col boolean,
          | double_col double,
          | float_col float,
          | x UUID,
          | ui8 UInt8,
          | ui16 UInt16,
          | ui32 UInt32,
          | ui64 UInt64,
          | ui128 UInt128,
          | ui256 UInt256,
          | i128 Int128,
          | i256 Int256)
          | ENGINE=File(TabSeparated)
          |""".stripMargin)
      statement.execute(
        """ insert into db1.type_test
          | (id, tiny_col, smallint_col, int_col, bigint_col, largeint_col, decimal_col,
          | date_col, datetime_col, char_col, varchar_col, string_col, boolean_col,
          | double_col, float_col, x, ui8, ui16, ui32, ui64, ui128, ui256, i128, i256)
          | VALUES (1, 2, 3, 4, 5, 6, 7.7,
          | '2022-05-08', '2022-05-08 17:47:45', 'a', 'Hello', 'Hello, Kyuubi', true,
          | 8.8, 9.9, generateUUIDv4(), 8, 16, 32, 64, 128, 256, -128, -256)
          |""".stripMargin)
      val resultSet1 = statement.executeQuery("select * from db1.type_test")
      while (resultSet1.next()) {
        assert(resultSet1.getObject(1) == 1)
        assert(resultSet1.getObject(2) == 2)
        assert(resultSet1.getObject(3) == 3)
        assert(resultSet1.getObject(4) == 4)
        assert(resultSet1.getObject(5) == 5)
        assert(resultSet1.getObject(6) == 6)
        assert(resultSet1.getObject(7) == new java.math.BigDecimal("7.700000000"))
        assert(resultSet1.getObject(8) == Date.valueOf("2022-05-08"))
        assert(resultSet1.getObject(9) == Timestamp.valueOf("2022-05-08 17:47:45"))
        assert(resultSet1.getObject(10) == "a")
        assert(resultSet1.getObject(11) == "Hello")
        assert(resultSet1.getObject(12) == "Hello, Kyuubi")
        assert(resultSet1.getObject(13) == true)
        assert(resultSet1.getObject(14) == 8.8)
        assert(resultSet1.getObject(15) == 9.9)
        assert(resultSet1.getString(16).length == 36)
        assert(resultSet1.getObject(17) == 8)
        assert(resultSet1.getObject(18) == 16)
        assert(resultSet1.getObject(19) == 32)
        assert(resultSet1.getObject(20) == "64")
        assert(resultSet1.getObject(21) == "128")
        assert(resultSet1.getObject(22) == "256")
        assert(resultSet1.getObject(23) == "-128")
        assert(resultSet1.getObject(24) == "-256")
      }
    }
  }

  test("clickhouse: test Array") {
    withJdbcStatement("test1") { statement =>
      val resultSet1 = statement.executeQuery("SELECT array(array(1,1),array(1,2))")
      while (resultSet1.next()) {
        val array = resultSet1.getObject(1)
        assert(array == "[[1, 1], [1, 2]]")
      }
    }
  }

  override protected def jdbcUrl: String = jdbcConnectionUrl
}
