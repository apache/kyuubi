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

package org.apache.kyuubi.server.statestore.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}
import java.util.Properties

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.statestore.StateStore

class JDBCStateStore(conf: KyuubiConf) extends StateStore with Logging {
  private var hikariConfig: HikariConfig = _
  private var hikariDataSource: HikariDataSource = _

  override def initialize(): Unit = {
    hikariConfig = new HikariConfig(new Properties())
    hikariDataSource = new HikariDataSource(hikariConfig)
  }

  override def shutdown(): Unit = {
    hikariDataSource.close()
  }

  private def getConnection(autoCommit: Boolean = true): Connection = {
    try {
      val connection = hikariDataSource.getConnection
      connection.setAutoCommit(autoCommit)
      connection
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    }
  }

  private def execute(conn: Connection, sql: String): Unit = {
    debug(s"executing sql $sql")
    var statement: Statement = null
    try {
      statement = conn.createStatement()
      statement.execute(sql)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (statement != null) {
        statement.close()
      }
    }
  }

  private def executeUpdate(conn: Connection, sql: String, params: _*): Int = {
    debug(s"executing update $sql")
    var result: Int = 0
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParam(statement, params)
      result = statement.executeUpdate()
      statement.closeOnCompletion()
    } catch onStatementError(statement)
    result
  }

  private def executeQuery(conn: Connection, sql: String, params: _*): ResultSet = {
    debug(s"executing update $sql")
    var rs: ResultSet = null
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParam(statement, params)
      rs = statement.executeQuery()
      statement.closeOnCompletion()
    } catch onStatementError(statement)
    rs
  }

  private def onStatementError(statement: Statement): PartialFunction[Throwable, Unit] = {
    case e: SQLException =>
      if (statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
      throw new KyuubiException(e.getMessage, e)
  }

  private def setStatementParam(statement: PreparedStatement, params: _*): Unit = {
    if (params != null) {
      params.zipWithIndex.map { case (param, index) =>
        param match {
          case s: String => statement.setString(index + 1, s)
          case i: Int => statement.setInt(index + 1, i)
          case d: Double => statement.setDouble(index + 1, d)
          case l: Long => statement.setLong(index + 1, l)
          case _ => throw new KyuubiException(s"Unsupported date type:${param.getClass.getName}")
        }

      }
    }
  }
}
