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

import java.sql.{DriverManager, SQLException, Statement}
import java.util.Locale

import org.apache.kyuubi.KyuubiFunSuite

trait JDBCTestHelper extends KyuubiFunSuite {
  // Load Driver class before using it, otherwise may cause the first call
  // `DriverManager.getConnection("jdbc:[protocol]://...")` failure.
  Class.forName(jdbcDriverClass)

  def jdbcDriverClass: String

  protected def user: String

  protected def password: String

  protected def defaultSchema = "default"

  protected def sessionConfigs: Map[String, String]

  protected def jdbcConfigs: Map[String, String]

  protected def jdbcVars: Map[String, String]

  protected def jdbcUrl: String

  protected def jdbcUrlWithConf: String = jdbcUrlWithConf(jdbcUrl)

  protected def jdbcUrlWithConf(jdbcUrl: String): String

  def assertJDBCConnectionFail(jdbcUrl: String = jdbcUrlWithConf): SQLException = {
    intercept[SQLException](DriverManager.getConnection(jdbcUrl, user, password))
  }

  def withMultipleConnectionJdbcStatement(
    tableNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrlWithConf, user, password) }
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
      info("Closing connections")
      connections.foreach(_.close())
      info("Closed connections")
    }
  }

  def withDatabases(dbNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrlWithConf, user, password) }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      dbNames.reverse.foreach { name =>
        statements.head.execute(s"DROP DATABASE IF EXISTS $name")
      }
      info("Closing statements")
      statements.foreach(_.close())
      info("Closed statements")
      info("Closing connections")
      connections.foreach(_.close())
      info("Closed connections")
    }
  }

  def withJdbcStatement(tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(tableNames: _*)(f)
  }
}
