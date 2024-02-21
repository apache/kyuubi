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
package org.apache.kyuubi.engine.jdbc.impala

import java.sql.{Date, Timestamp}

import org.apache.kyuubi.operation.HiveJDBCTestHelper

class StatementSuite extends WithImpalaEngine with HiveJDBCTestHelper {

  test("impala - test select") {
    withJdbcStatement("test1") { statement =>
      statement.execute("create table test1(id bigint, " +
        "name string, age integer)")
      statement.execute("insert into test1 values(1, 'a', 11)")

      val resultSet1 = statement.executeQuery("select * from test1")
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

  test("impala - test types") {
    withJdbcStatement("type_test") { statement =>
      statement.execute("create table type_test(" +
        "id bigint, " +
        "smallint_col smallint, " +
        "int_col int, " +
        "bigint_col bigint, " +
        "date_col date, " +
        "timestamp_col timestamp, " +
        "char_col char(10), " +
        "varchar_col varchar(255), " +
        "boolean_col boolean, " +
        "double_col double, " +
        "real_col real, " +
        "string_col STRING " +
        ")")

      statement.execute("insert into type_test" +
        "(id, " +
        "smallint_col, " +
        "int_col, " +
        "bigint_col, " +
        "date_col, " +
        "timestamp_col, " +
        "char_col, " +
        "varchar_col, " +
        "boolean_col, " +
        "double_col, " +
        "real_col," +
        "string_col) " +
        "VALUES (1, " +
        "2, " +
        "3, " +
        "4, " +
        "'2022-05-08', " +
        "'2022-05-08 17:47:45'," +
        "CAST('a' AS char(10)), " +
        "CAST('Hello' AS varchar(255)), " +
        "true, " +
        "8.8, " +
        "9.9, " +
        "'test_str' " +
        ")")

      val resultSet1 = statement.executeQuery("select * from type_test")
      while (resultSet1.next()) {
        assert(resultSet1.getObject(1) == 1)
        assert(resultSet1.getObject(2) == 2)
        assert(resultSet1.getObject(3) == 3)
        assert(resultSet1.getObject(4) == 4)
        assert(resultSet1.getObject(5) == Date.valueOf("2022-05-08"))
        assert(resultSet1.getObject(6) == Timestamp.valueOf("2022-05-08 17:47:45"))
        assert(resultSet1.getString(7).trim == "a")
        assert(resultSet1.getObject(8) == "Hello")
        assert(resultSet1.getObject(9) == true)
        assert(resultSet1.getObject(10) == 8.8)
        assert(resultSet1.getObject(11) == 9.9)
        assert(resultSet1.getObject(12) == "test_str")
      }
    }
  }

  override protected def jdbcUrl: String = jdbcConnectionUrl
}
