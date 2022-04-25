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

class StatementSuite extends WithMysqlEngine {

  test("test update") {
    withJdbcStatement("test1") { statement =>
      statement.execute("create table test1(id bigint, name varchar(255), age int)")
      statement.execute("insert into test1 values(1, 'a', 11)")
      statement.executeQuery(
        "update test1 set name = 'test', age = 12 where id = 1")

      val resultSet1 = statement.executeQuery("select * from test1")
      while (resultSet1.next()) {
        val id = resultSet1.getObject(1)
        assert(id == 1)
        val name = resultSet1.getObject(2)
        assert(name == "test")
        val age = resultSet1.getObject(3)
        assert(age == 12)
      }
    }
  }

}
