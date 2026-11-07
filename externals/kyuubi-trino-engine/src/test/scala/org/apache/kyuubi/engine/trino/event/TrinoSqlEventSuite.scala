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
package org.apache.kyuubi.engine.trino.event

import java.io.{BufferedReader, InputStreamReader}
import java.net.InetAddress

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_EVENT_JSON_LOG_PATH, ENGINE_SHARE_LEVEL, ENGINE_TRINO_CONNECTION_CATALOG}
import org.apache.kyuubi.engine.trino.WithTrinoEngine
import org.apache.kyuubi.events.JsonProtocol
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.ServiceState

class TrinoSqlEventSuite extends WithTrinoEngine with HiveJDBCTestHelper {

  private val logRoot = kyuubiConf.get(ENGINE_EVENT_JSON_LOG_PATH)
  private val hostName = InetAddress.getLocalHost.getCanonicalHostName

  // Read every day partition so the test does not depend on the date the suite was constructed,
  // which can differ from the date the events are written if a run crosses midnight.
  private def readEventLines(eventType: String): Seq[String] = {
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val eventFiles = Option(fileSystem.globStatus(
      new Path(logRoot, s"$eventType/day=*/Trino-$hostName.json"))).toSeq.flatten
    eventFiles.sortBy(_.getPath.toString).flatMap { eventFile =>
      val in = fileSystem.open(eventFile.getPath)
      try {
        val reader = new BufferedReader(new InputStreamReader(in))
        Iterator.continually(reader.readLine()).takeWhile(_ != null).toList
      } finally {
        in.close()
      }
    }
  }

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_TRINO_CONNECTION_CATALOG.key -> "memory",
    ENGINE_SHARE_LEVEL.key -> "SERVER")

  override protected val schema = "default"

  override protected def jdbcUrl: String = getJdbcUrl

  override def afterAll(): Unit = {
    super.afterAll()
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val logPath = new Path(logRoot)
    if (fileSystem.exists(logPath)) {
      fileSystem.delete(logPath, true)
    }
    fileSystem.close()
  }

  test("test engine event logging") {
    val engineEvents = readEventLines("trino_engine")
    val initializedEvent =
      JsonProtocol.jsonToEvent(engineEvents.head, classOf[TrinoEngineEvent])
    assert(initializedEvent.asInstanceOf[TrinoEngineEvent].state.equals(ServiceState.INITIALIZED))

    val startedEvent =
      JsonProtocol.jsonToEvent(engineEvents(1), classOf[TrinoEngineEvent])
    assert(startedEvent.asInstanceOf[TrinoEngineEvent].state.equals(ServiceState.STARTED))
  }

  test("test session event logging") {
    withJdbcStatement() { statement =>
      val catalogs = statement.getConnection.getMetaData.getCatalogs
      assert(catalogs.next())
      val sessionEvents = readEventLines("trino_session")
      val readEvent =
        JsonProtocol.jsonToEvent(sessionEvents.head, classOf[TrinoSessionEvent])
      assert(readEvent.isInstanceOf[TrinoSessionEvent])
    }
  }

  test("test operation event logging") {
    withJdbcStatement() { statement =>
      val createSchemaStatement = "CREATE SCHEMA IF NOT EXISTS memory.test_schema"
      statement.execute(createSchemaStatement)
      val createTableStatement = "CREATE TABLE IF NOT EXISTS " +
        "memory.test_schema.trino_engine_test(id int)"
      statement.execute(createTableStatement)
      val insertStatement = "INSERT INTO memory.test_schema.trino_engine_test SELECT 1"
      statement.execute(insertStatement)

      val operationEvents = readEventLines("trino_operation")

      val createSchemaEvent =
        JsonProtocol.jsonToEvent(operationEvents.head, classOf[TrinoOperationEvent])
      assert(createSchemaEvent.isInstanceOf[TrinoOperationEvent])
      assert(StringUtils.equals(
        createSchemaEvent.asInstanceOf[TrinoOperationEvent].statement,
        createSchemaStatement))

      val createTableEvent =
        JsonProtocol.jsonToEvent(operationEvents(1), classOf[TrinoOperationEvent])
      assert(createTableEvent.isInstanceOf[TrinoOperationEvent])
      assert(StringUtils.equals(
        createTableEvent.asInstanceOf[TrinoOperationEvent].statement,
        createTableStatement))

      val insertEvent =
        JsonProtocol.jsonToEvent(operationEvents(2), classOf[TrinoOperationEvent])
      assert(insertEvent.isInstanceOf[TrinoOperationEvent])
      assert(StringUtils.equals(
        insertEvent.asInstanceOf[TrinoOperationEvent].statement,
        insertStatement))
    }
  }

}
