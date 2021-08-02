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

package org.apache.kyuubi.engine.spark.events

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.JDBCTestUtils

class EventLoggingServiceSuite extends WithSparkSQLEngine with JDBCTestUtils {
  private val logRoot = Utils.createTempDir()
  override def withKyuubiConf: Map[String, String] = Map(
    KyuubiConf.ENGINE_EVENT_LOGGERS.key -> EventLoggerType.JSON.toString,
    KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH.key -> logRoot.toString
  )

  override protected def jdbcUrl: String = getJdbcUrl


  test("round-trip for event logging service") {
    val engineEventPath = Paths.get(logRoot.toString, "engine", engine.engineId + ".json")
    val reader = Files.newBufferedReader(engineEventPath, StandardCharsets.UTF_8)

    val a = JsonProtocol.jsonToEvent(reader.readLine())
    assert(a.isInstanceOf[KyuubiEvent])

    withJdbcStatement() { statement =>
      val table = engineEventPath.getParent.toString
      val resultSet = statement.executeQuery(s"SELECT * FROM `json`.`${table}`")
      while (resultSet.next()) {
        assert(resultSet.getString("Event") === classOf[EngineEvent].getCanonicalName)
        assert(resultSet.getString("applicationId") === spark.sparkContext.applicationId)
        assert(resultSet.getString("master") === spark.sparkContext.master)
      }
    }
  }

}
