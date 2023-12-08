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
package org.apache.kyuubi.engine.jdbc.mysql

import java.sql.{Date, Timestamp}

import org.apache.kyuubi.operation.HiveJDBCTestHelper

class StatementSuite extends WithMySQLEngine with HiveJDBCTestHelper {

  test("test select") {
    withJdbcStatement("test1") { statement =>
      statement.execute("create database if not exists db1")
      statement.execute("use db1")
      statement.execute("create table db1.test1(id bigint, name varchar(255), age int, " +
        "PRIMARY KEY ( `id` ))" +
        "ENGINE=InnoDB " +
        "DEFAULT CHARSET=utf8;")
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

  test("test types") {
    withJdbcStatement("test1") { statement =>
      statement.execute("create database if not exists db1")
      statement.execute("use db1")
      statement.execute("create table db1.type_test(" +
        "id bigint, " +
        "tiny_col tinyint, smallint_col smallint, " +
        "int_col int, bigint_col bigint, " +
        "decimal_col decimal(27, 9)," +
        "date_col date, datetime_col datetime, timestamp_col timestamp," +
        "char_col char, varchar_col varchar(255),  " +
        "boolean_col boolean, " +
        "double_col double, float_col float," +
        "PRIMARY KEY ( `id` )) " +
        "ENGINE=InnoDB " +
        "DEFAULT CHARSET=utf8")
      statement.execute("insert into db1.type_test" +
        "(id, " +
        "tiny_col, smallint_col, int_col, bigint_col, " +
        "decimal_col, " +
        "date_col, datetime_col, timestamp_col," +
        "char_col, varchar_col, " +
        "boolean_col, " +
        "double_col, float_col) " +
        "VALUES (1, 2, 3, 4, 5, 6.6, '2023-10-23', '2023-10-23 15:31:45', " +
        "'2023-10-23 15:31:45', 'a', 'Hello', true, 7.7, 8.8)")

      val resultSet1 = statement.executeQuery("select * from db1.type_test")
      while (resultSet1.next()) {
        assert(resultSet1.getObject(1) == 1)
        assert(resultSet1.getObject(2) == 2)
        assert(resultSet1.getObject(3) == 3)
        assert(resultSet1.getObject(4) == 4)
        assert(resultSet1.getObject(5) == 5)
        assert(resultSet1.getObject(6) == new java.math.BigDecimal("6.600000000"))
        assert(resultSet1.getObject(7) == Date.valueOf("2023-10-23"))
        assert(resultSet1.getObject(8) == Timestamp.valueOf("2023-10-23 15:31:45"))
        assert(resultSet1.getObject(9) == Timestamp.valueOf("2023-10-23 15:31:45"))
        assert(resultSet1.getObject(10) == "a")
        assert(resultSet1.getObject(11) == "Hello")
        assert(resultSet1.getObject(12) == true)
        assert(resultSet1.getObject(13) == 7.7)
        assert(resultSet1.getObject(14) == 8.8)
      }
    }
  }

  override protected def jdbcUrl: String = jdbcConnectionUrl
}
