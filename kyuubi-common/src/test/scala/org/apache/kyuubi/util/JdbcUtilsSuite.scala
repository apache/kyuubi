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

package org.apache.kyuubi.util

import java.util.Properties
import javax.sql.DataSource

import com.zaxxer.hikari.util.DriverDataSource

import org.apache.kyuubi.KyuubiFunSuite

class JdbcUtilsSuite extends KyuubiFunSuite {

  private val connUrl = "jdbc:sqlite:file:test_jdbc.db"
  private val driverClz = "org.sqlite.JDBC"

  implicit private val ds: DataSource =
    new DriverDataSource(connUrl, driverClz, new Properties, "test", "test")

  case class Person(id: Int, name: String)

  test("JdbcUtils methods") {
    JdbcUtils.execute("DROP TABLE IF EXISTS person")()
    JdbcUtils.execute(
      """CREATE TABLE person(
        |  id   INT NOT NULL PRIMARY KEY,
        |  name VARCHAR(255)
        |)
        |""".stripMargin)()
    val affected = JdbcUtils.executeUpdate("INSERT INTO person VALUES (?, ?), (?, ?)") { stmt =>
      stmt.setInt(1, 3)
      stmt.setString(2, "Apache")
      stmt.setInt(3, 9)
      stmt.setString(4, "Kyuubi")
    }
    assert(affected == 2)

    val persons = JdbcUtils.executeQueryWithRowMapper {
      "SELECT * FROM person WHERE id=?"
    } { stmt =>
      stmt.setInt(1, 9)
    } { rs =>
      Person(rs.getInt(1), rs.getString(2))
    }
    assert(persons.length == 1)
    assert(persons.head == Person(9, "Kyuubi"))

    JdbcUtils.executeQuery("SELECT count(*) FROM person")() { rs =>
      assert(rs.next())
      assert(!rs.next())
    }

    assertResult("****************(length:16)") {
      JdbcUtils.redactPassword(Some("sample_pass_word"))
    }

    assertResult("(empty)") {
      JdbcUtils.redactPassword(Some(""))
    }

    assertResult("(empty)") {
      JdbcUtils.redactPassword(None)
    }

    JdbcUtils.execute("DROP TABLE IF EXISTS person")()
  }
}
