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

import java.sql.{DriverManager, ResultSet, Statement}
import java.util.Locale

import org.apache.hive.service.rpc.thrift.{TCLIService, TCloseSessionReq, TOpenSessionReq, TSessionHandle}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.service.authentication.PlainSASLHelper

trait JDBCTestUtils extends KyuubiFunSuite {

  protected val dftSchema = "default"
  protected val user: String = Utils.currentUser
  protected val patterns = Seq("", "*", "%", null, ".*", "_*", "_%", ".%")
  protected def jdbcUrl: String
  protected def configMap: Map[String, String] = Map.empty

  private def jdbcUrlWithConf: String = {
    jdbcUrl + configMap.map(kv => kv._1 + "=" + kv._2).mkString(";")
  }

  def withMultipleConnectionJdbcStatement(
      tableNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrlWithConf, user, "") }
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

  def withDatabases(dbNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrlWithConf, user, "") }
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
      connections.foreach(_.close())
      info("Closing connections")
    }
  }

  def withJdbcStatement(tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(tableNames: _*)(f)
  }

  def withThriftClient(f: TCLIService.Iface => Unit): Unit = {
    val hostAndPort = jdbcUrl.stripPrefix("jdbc:hive2://").split("/;").head.split(":")
    val host = hostAndPort.head
    val port = hostAndPort(1).toInt
    val socket = new TSocket(host, port)
    val transport = PlainSASLHelper.getPlainTransport(Utils.currentUser, "anonymous", socket)

    val protocol = new TBinaryProtocol(transport)
    val client = new TCLIService.Client(protocol)
    transport.open()
    try {
      f(client)
    } finally {
      socket.close()
    }
  }

  def withSessionHandle(f: (TCLIService.Iface, TSessionHandle) => Unit): Unit = {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      val resp = client.OpenSession(req)
      val handle = resp.getSessionHandle

      try {
        f(client, handle)
      } finally {
        val tCloseSessionReq = new TCloseSessionReq(handle)
        try {
          client.CloseSession(tCloseSessionReq)
        } catch {
          case e: Exception => error(s"Failed to close $handle", e)
        }
      }
    }
  }

  def checkGetSchemas(
      rs: ResultSet, dbNames: Seq[String], catalogName: String = ""): Unit = {
    var count = 0
    while(rs.next()) {
      count += 1
      assert(dbNames.contains(rs.getString("TABLE_SCHEM")))
      assert(rs.getString("TABLE_CATALOG") === catalogName)
    }
    // Make sure there are no more elements
    assert(!rs.next())
    assert(dbNames.size === count, "All expected schemas should be visited")
  }
}
