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
import java.nio.file.Paths

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_EVENT_JSON_LOG_PATH, ENGINE_SHARE_LEVEL, ENGINE_TRINO_CONNECTION_CATALOG}
import org.apache.kyuubi.engine.trino.WithTrinoEngine
import org.apache.kyuubi.events.JsonProtocol
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.ServiceState

class TrinoSqlEventSuite extends WithTrinoEngine with HiveJDBCTestHelper {

  private val logRoot = kyuubiConf.get(ENGINE_EVENT_JSON_LOG_PATH)
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())
  private val hostName = InetAddress.getLocalHost.getCanonicalHostName

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
    val engineEventPath = Paths.get(
      logRoot,
      "trino_engine",
      s"day=$currentDate",
      s"Trino-$hostName.json")
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val fs: FSDataInputStream = fileSystem.open(new Path(engineEventPath.toString))
    val engineEventReader = new BufferedReader(new InputStreamReader(fs))
    val initializedEvent =
      JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[TrinoEngineEvent])
    assert(initializedEvent.asInstanceOf[TrinoEngineEvent].state.equals(ServiceState.INITIALIZED))

    val startedEvent =
      JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[TrinoEngineEvent])
    assert(startedEvent.asInstanceOf[TrinoEngineEvent].state.equals(ServiceState.STARTED))
  }

  test("test session event logging") {
    withJdbcStatement() { statement =>
      val catalogs = statement.getConnection.getMetaData.getCatalogs
      assert(catalogs.next())
      val sessionEventPath = Paths.get(
        logRoot,
        "trino_session",
        s"day=$currentDate",
        s"Trino-$hostName.json")
      val fileSystem: FileSystem = FileSystem.get(new Configuration())
      val fs: FSDataInputStream = fileSystem.open(new Path(sessionEventPath.toString))
      val engineEventReader = new BufferedReader(new InputStreamReader(fs))
      val readEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[TrinoSessionEvent])
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

      val operationEventPath = Paths.get(
        logRoot,
        "trino_operation",
        s"day=$currentDate",
        s"Trino-$hostName.json")

      val fileSystem: FileSystem = FileSystem.get(new Configuration())
      val fs: FSDataInputStream = fileSystem.open(new Path(operationEventPath.toString))
      val engineEventReader = new BufferedReader(new InputStreamReader(fs))

      val createSchemaEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[TrinoOperationEvent])
      assert(createSchemaEvent.isInstanceOf[TrinoOperationEvent])
      assert(StringUtils.equals(
        createSchemaEvent.asInstanceOf[TrinoOperationEvent].statement,
        createSchemaStatement))

      val createTableEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[TrinoOperationEvent])
      assert(createTableEvent.isInstanceOf[TrinoOperationEvent])
      assert(StringUtils.equals(
        createTableEvent.asInstanceOf[TrinoOperationEvent].statement,
        createTableStatement))

      val insertEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[TrinoOperationEvent])
      assert(insertEvent.isInstanceOf[TrinoOperationEvent])
      assert(StringUtils.equals(
        insertEvent.asInstanceOf[TrinoOperationEvent].statement,
        insertStatement))
    }
  }

}
