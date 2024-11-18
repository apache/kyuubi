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
package org.apache.kyuubi.engine.jdbc.oracle

import java.sql.Timestamp

import org.apache.kyuubi.operation.HiveJDBCTestHelper

class OracleStatementSuite extends WithOracleEngine with HiveJDBCTestHelper {

  test("oracle - test select") {
    withJdbcStatement() { statement =>
      statement.execute(
        """create table T_TEST
          |(
          |    ID   INTEGER not null
          |        constraint "T_TEST_PK" primary key,
          |    NAME VARCHAR2(64),
          |    USER_ID LONG,
          |    SCORE NUMBER(8,2)
          |)""".stripMargin)
      statement.execute("""INSERT INTO T_TEST(ID, NAME, USER_ID, SCORE)
                          |VALUES (1, 'Bob', 43254353,89.92)""".stripMargin)
      val resultSet = statement.executeQuery("SELECT * FROM T_TEST")
      while (resultSet.next()) {
        val id = resultSet.getObject(1)
        assert(id == 1)
        val name = resultSet.getObject(2)
        assert(name == "Bob")
        val user_id = resultSet.getObject(3)
        assert(user_id == "43254353")
        val score = resultSet.getObject(4)
        assert(score == new java.math.BigDecimal("89.92"))
      }
      statement.execute("DROP TABLE T_TEST")
    }
  }

  test("oracle - test types") {
    withJdbcStatement() { statement =>
      statement.execute(
        """
          |CREATE TABLE TYPE_TEST
          |(
          |    INT_COL       INTEGER,
          |    NUM_8_COL     NUMBER(8, 0),
          |    NUM_16_4_COL  NUMBER(16, 4),
          |    DECIMAL_COL   DECIMAL(8, 2),
          |    DATE_COL      DATE,
          |    TIMESTAMP_COL TIMESTAMP,
          |    CHAR_COL      CHAR(10),
          |    VARCHAR2_COL  VARCHAR2(255),
          |    NCHAR_COL     NCHAR(10),
          |    NCHAR2_COL    NVARCHAR2(255),
          |    LONG_COL      LONG,
          |    FLOAT_COL     FLOAT,
          |    REAL_COL      REAL
          |)
          |""".stripMargin)
      statement.execute(
        """
          |INSERT INTO TYPE_TEST( INT_COL
          |                     , NUM_8_COL
          |                     , NUM_16_4_COL
          |                     , DECIMAL_COL
          |                     , DATE_COL
          |                     , TIMESTAMP_COL
          |                     , CHAR_COL
          |                     , VARCHAR2_COL
          |                     , NCHAR_COL
          |                     , NCHAR2_COL
          |                     , LONG_COL
          |                     , FLOAT_COL
          |                     , REAL_COL)
          |VALUES ( 1
          |       , 2
          |       , 3.1415
          |       , 0.61
          |       , TO_DATE('2024-11-07', 'YYYY-MM-DD')
          |       , TO_TIMESTAMP('2024-11-07 22:03:01.324', 'YYYY-MM-DD HH24:MI:SS.FF3')
          |    , 'pi'
          |    , 'alice'
          |    , 'bob'
          |    , 'siri'
          |    , 'alex'
          |    , 1.432
          |    , 3.432
          |       )
          |""".stripMargin)

      val resultSet1 = statement.executeQuery("SELECT * FROM TYPE_TEST")
      while (resultSet1.next()) {
        assert(resultSet1.getObject(1) == 1)
        assert(resultSet1.getObject(2) == 2)
        assert(resultSet1.getObject(3) == new java.math.BigDecimal("3.1415"))
        assert(resultSet1.getObject(4) == new java.math.BigDecimal("0.61"))
        assert(resultSet1.getObject(5) == Timestamp.valueOf("2024-11-07 00:00:00"))
        assert(resultSet1.getObject(6) == Timestamp.valueOf("2024-11-07 22:03:01.324"))
        assert(resultSet1.getObject(7) == "pi        ")
        assert(resultSet1.getObject(8) == "alice")
        assert(resultSet1.getObject(9) == "bob       ")
        assert(resultSet1.getObject(10) == "siri")
        assert(resultSet1.getObject(11) == "alex")
        assert(resultSet1.getObject(12) == new java.math.BigDecimal("1.432"))
        assert(resultSet1.getObject(13) == new java.math.BigDecimal("3.432"))
      }
      statement.execute("DROP TABLE TYPE_TEST")
    }
  }

  override protected def jdbcUrl: String = jdbcConnectionUrl
}
