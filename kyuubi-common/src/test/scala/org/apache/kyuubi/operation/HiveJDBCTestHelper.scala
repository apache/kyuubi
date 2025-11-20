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

import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.Utils
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TOperationState._

trait HiveJDBCTestHelper extends JDBCTestHelper {

  def jdbcDriverClass: String = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"

  protected val URL_PREFIX = "jdbc:hive2://"

  protected def matchAllPatterns = Seq("", "*", "%", null, ".*", "_*", "_%", ".%")

  override protected lazy val user: String = Utils.currentUser
  override protected val password = "anonymous"
  private var _sessionConfigs: Map[String, String] = Map.empty
  private var _jdbcConfigs: Map[String, String] = Map.empty
  private var _jdbcVars: Map[String, String] = Map.empty

  override protected def sessionConfigs: Map[String, String] = _sessionConfigs

  override protected def jdbcConfigs: Map[String, String] = _jdbcConfigs

  override protected def jdbcVars: Map[String, String] = _jdbcVars

  def withSessionConf[T](
      sessionConfigs: Map[String, String] = Map.empty)(
      jdbcConfigs: Map[String, String] = Map.empty)(
      jdbcVars: Map[String, String] = Map.empty)(f: => T): T = {
    this._sessionConfigs = sessionConfigs
    this._jdbcConfigs = jdbcConfigs
    this._jdbcVars = jdbcVars
    try f
    finally {
      _jdbcVars = Map.empty
      _jdbcConfigs = Map.empty
      _sessionConfigs = Map.empty
    }
  }

  override protected def jdbcUrlWithConf(jdbcUrl: String): String = {
    val sessionConfStr = sessionConfigs.map(kv => kv._1 + "=" + kv._2).mkString(";")
    val jdbcConfStr =
      if (jdbcConfigs.isEmpty) {
        ""
      } else {
        "?" + jdbcConfigs.map(kv => kv._1 + "=" + kv._2).mkString(";")
      }
    val jdbcVarsStr =
      if (jdbcVars.isEmpty) {
        ""
      } else {
        "#" + jdbcVars.map(kv => kv._1 + "=" + kv._2).mkString(";")
      }
    jdbcUrl + sessionConfStr + jdbcConfStr + jdbcVarsStr
  }

  def withThriftClient[T](f: TCLIService.Iface => T): T = {
    withThriftClient()(f)
  }

  def withThriftClient[T](user: Option[String] = None)(f: TCLIService.Iface => T): T = {
    TClientTestUtils.withThriftClient(
      jdbcUrl.stripPrefix(URL_PREFIX).split("/;").head,
      user)(f)
  }

  def withThriftClientAndConnectionConf[T](f: (TCLIService.Iface, Map[String, String]) => T): T = {
    withThriftClientAndConnectionConf()(f)
  }

  def withThriftClientAndConnectionConf[T](user: Option[String] = None)(f: (
      TCLIService.Iface,
      Map[String, String]) => T): T = {
    TClientTestUtils.withThriftClientAndConnectionConf(
      jdbcUrl.stripPrefix(URL_PREFIX),
      user)(f)
  }

  def withSessionHandle[T](f: (TCLIService.Iface, TSessionHandle) => T): T = {
    val hostAndPort = jdbcUrl.stripPrefix(URL_PREFIX).split("/;").head
    TClientTestUtils.withSessionHandle(hostAndPort, sessionConfigs)(f)
  }

  def withSessionAndLaunchEngineHandle[T](
      f: (TCLIService.Iface, TSessionHandle, Option[TOperationHandle]) => T): T = {
    val hostAndPort = jdbcUrl.stripPrefix(URL_PREFIX).split("/;").head
    TClientTestUtils.withSessionAndLaunchEngineHandle(hostAndPort, sessionConfigs)(f)
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
    assertOperationStatusIn(
      client,
      op,
      Set(
        FINISHED_STATE,
        CANCELED_STATE,
        CLOSED_STATE,
        ERROR_STATE,
        UKNOWN_STATE,
        TIMEDOUT_STATE),
      90)
  }

  def assertOperationStatusIn(
      client: Iface,
      op: TOperationHandle,
      status: Set[TOperationState],
      timeoutInSeconds: Int): Unit = {
    eventually(timeout(timeoutInSeconds.seconds), interval(100.milliseconds)) {
      val state = client.GetOperationStatus(new TGetOperationStatusReq(op)).getOperationState
      assert(status.contains(state))
    }
  }

}
