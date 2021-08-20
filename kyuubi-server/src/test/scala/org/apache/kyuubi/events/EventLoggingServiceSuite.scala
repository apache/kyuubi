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

package org.apache.kyuubi.events

import java.net.InetAddress
import java.nio.file.Paths

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.JDBCTestUtils
import org.apache.kyuubi.operation.OperationState._

class EventLoggingServiceSuite extends WithKyuubiServer with JDBCTestUtils {
  private val logRoot = Utils.createTempDir()
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("JSON"))
      .set(KyuubiConf.SERVER_EVENT_JSON_LOG_PATH, logRoot.toString)
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("statementEvent: generate, dump and query") {
    val hostName = InetAddress.getLocalHost.getCanonicalHostName
    val statementEventPath = Paths.get(logRoot.toString, "statement", hostName + ".json")
    val sql = "select timestamp'2021-06-01'"

    withJdbcStatement() { statement =>
      statement.execute(sql)
      val table = statementEventPath.getParent
      val resultSet = statement.executeQuery(s"SELECT * FROM `json`.`${table}`" +
        "where statement = \"" + sql + "\"")
      val states = Array(INITIALIZED, PENDING, RUNNING, FINISHED, CLOSED)
      var stateIndex = 0
      while (resultSet.next()) {
        assert(resultSet.getString("user") == Utils.currentUser)
        assert(resultSet.getString("statement") == sql)
        assert(resultSet.getString("state") == states(stateIndex).toString)
        stateIndex += 1
      }
    }
  }
}
