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
package org.apache.kyuubi.engine.jdbc.postgresql

import java.sql.{Date, Timestamp}

import org.apache.kyuubi.operation.HiveJDBCTestHelper

class StatementSuite extends WithPostgreSqlEngine with HiveJDBCTestHelper {

  test("test select") {
    withJdbcStatement("test1") { statement =>
      statement.execute("create table public.test1(id bigint primary key, " +
        "name varchar(255), age integer)")
      statement.execute("insert into public.test1 values(1, 'a', 11)")

      val resultSet1 = statement.executeQuery("select * from public.test1")
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
    withJdbcStatement("type_test") { statement =>
      statement.execute("create table public.type_test(" +
        "id bigint primary key, " +
        "smallint_col smallint, " +
        "int_col integer, " +
        "bigint_col bigint, " +
//        "decimal_col decimal(27, 9), " +
        "date_col date, " +
        "timestamp_col timestamp, " +
        "char_col char(10), " +
        "varchar_col varchar(255), " +
        "boolean_col boolean, " +
        "double_col double precision, " +
        "float_col float)")
      statement.execute("insert into public.type_test" +
        "(id, " +
        "smallint_col, " +
        "int_col, " +
        "bigint_col, " +
//                "decimal_col, " +
        "date_col, " +
        "timestamp_col, " +
        "char_col, " +
        "varchar_col, " +
        "boolean_col, " +
        "double_col, " +
        "float_col) " +
        "VALUES (1, " +
        "2, " +
        "3, " +
        "4, " +
//                "7.7, " +
        "'2022-05-08', " +
        "'2022-05-08 17:47:45'," +
        "'a', " +
        "'Hello', " +
        "true, " +
        "8.8, " +
        "9.9)")

      val resultSet1 = statement.executeQuery("select * from public.type_test")
      while (resultSet1.next()) {
        val id = resultSet1.getObject(1)
        assert(resultSet1.getObject(1) == 1)
        assert(resultSet1.getObject(2) == 2)
        assert(resultSet1.getObject(3) == 3)
        assert(resultSet1.getObject(4) == 4)
//                assert(resultSet1.getObject(5) == new java.math.BigDecimal("7.7"))
        assert(resultSet1.getObject(5) == Date.valueOf("2022-05-08"))
        assert(resultSet1.getObject(6) == Timestamp.valueOf("2022-05-08 17:47:45"))
        assert(resultSet1.getString(7).trim == "a")
        assert(resultSet1.getObject(8) == "Hello")
        assert(resultSet1.getObject(9) == true)
        assert(resultSet1.getObject(10) == 8.8)
        assert(resultSet1.getObject(11) == 9.9)
      }
    }
  }

  override protected def jdbcUrl: String = jdbcConnectionUrl
}
