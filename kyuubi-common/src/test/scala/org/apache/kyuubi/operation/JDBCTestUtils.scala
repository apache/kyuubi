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

import java.sql.{DriverManager, Statement}
import java.util.Locale

import org.apache.kyuubi.{KyuubiFunSuite, Utils}

trait JDBCTestUtils extends KyuubiFunSuite {

  protected val dftSchema = "default"
  protected val user: String = Utils.currentUser
  protected def jdbcUrl: String

  protected def withMultipleConnectionJdbcStatement(
      tableNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrl, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      tableNames.foreach { name =>
        if (name.toUpperCase(Locale.ROOT).startsWith("VIEW")) {
          statements.head.execute(s"DROP VIEW IF EXISTS $name")
        } else {
          statements.head.execute(s"DROP TABLE IF EXISTS $name")
        }
      }
      info("Closing statements")
      statements.foreach(_.close())
      info("Closed statements")
      connections.foreach(_.close())
      info("Closing connections")
    }
  }

  protected def withDatabases(dbNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrl, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      dbNames.foreach { name =>
        statements.head.execute(s"DROP DATABASE IF EXISTS $name")
      }
      info("Closing statements")
      statements.foreach(_.close())
      info("Closed statements")
      connections.foreach(_.close())
      info("Closing connections")
    }
  }

  protected def withJdbcStatement(tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(tableNames: _*)(f)
  }
}
