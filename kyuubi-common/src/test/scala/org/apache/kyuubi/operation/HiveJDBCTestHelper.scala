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

import java.sql.ResultSet

import org.apache.hive.service.rpc.thrift._
import org.apache.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.hive.service.rpc.thrift.TOperationState._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.Utils
import org.apache.kyuubi.service.authentication.PlainSASLHelper

trait HiveJDBCTestHelper extends JDBCTestHelper {

  def jdbcDriverClass: String = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"

  protected def matchAllPatterns = Seq("", "*", "%", null, ".*", "_*", "_%", ".%")

  protected override lazy val user: String = Utils.currentUser
  protected override val password = "anonymous"
  private var _sessionConfigs: Map[String, String] = Map.empty
  private var _jdbcConfigs: Map[String, String] = Map.empty
  private var _jdbcVars: Map[String, String] = Map.empty

  protected override def sessionConfigs: Map[String, String] = _sessionConfigs

  protected override def jdbcConfigs: Map[String, String] = _jdbcConfigs

  protected override def jdbcVars: Map[String, String] = _jdbcVars

  def withSessionConf[T](
      sessionConfigs: Map[String, String] = Map.empty)(
      jdbcConfigs: Map[String, String])(
      jdbcVars: Map[String, String])(f: => T): T = {
    this._sessionConfigs = sessionConfigs
    this._jdbcConfigs = jdbcConfigs
    this._jdbcVars = jdbcVars
    try f finally {
      _jdbcVars = Map.empty
      _jdbcConfigs = Map.empty
      _sessionConfigs = Map.empty
    }
  }

  protected override def jdbcUrlWithConf(jdbcUrl: String): String = {
    val sessionConfStr = sessionConfigs.map(kv => kv._1 + "=" + kv._2).mkString(";")
    val jdbcConfStr = if (jdbcConfigs.isEmpty) {
      ""
    } else {
      "?" + jdbcConfigs.map(kv => kv._1 + "=" + kv._2).mkString(";")
    }
    val jdbcVarsStr = if (jdbcVars.isEmpty) {
      ""
    } else {
      "#" + jdbcVars.map(kv => kv._1 + "=" + kv._2).mkString(";")
    }
    jdbcUrl + sessionConfStr + jdbcConfStr + jdbcVarsStr
  }

  def withThriftClient(f: TCLIService.Iface => Unit): Unit = {
    val hostAndPort = jdbcUrl.stripPrefix("jdbc:hive2://").split("/;").head.split(":")
    val host = hostAndPort.head
    val port = hostAndPort(1).toInt
    val socket = new TSocket(host, port)
    val transport = PlainSASLHelper.getPlainTransport(Utils.currentUser, password, socket)

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
      req.setPassword(password)
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

  def checkGetSchemas(rs: ResultSet, dbNames: Seq[String], catalogName: String = ""): Unit = {
    var count = 0
    while (rs.next()) {
      count += 1
      assert(dbNames.contains(rs.getString("TABLE_SCHEM")))
      assert(rs.getString("TABLE_CATALOG") === catalogName)
    }
    // Make sure there are no more elements
    assert(!rs.next())
    assert(dbNames.size === count, "All expected schemas should be visited")
  }

  def waitForOperationToComplete(client: Iface, op: TOperationHandle): Unit = {
    val req = new TGetOperationStatusReq(op)
    var state = client.GetOperationStatus(req).getOperationState
    eventually(timeout(90.seconds), interval(100.milliseconds)) {
      state = client.GetOperationStatus(req).getOperationState
      assert(!Set(INITIALIZED_STATE, PENDING_STATE, RUNNING_STATE).contains(state))
    }
  }

  def sparkEngineMajorMinorVersion: (Int, Int) = {
    var sparkRuntimeVer = ""
    withJdbcStatement() { stmt =>
      val result = stmt.executeQuery("SELECT version()")
      assert(result.next())
      sparkRuntimeVer = result.getString(1)
      assert(!result.next())
    }
    Utils.majorMinorVersion(sparkRuntimeVer)
  }
}
