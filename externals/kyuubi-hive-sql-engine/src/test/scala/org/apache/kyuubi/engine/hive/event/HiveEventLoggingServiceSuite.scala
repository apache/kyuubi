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
package org.apache.kyuubi.engine.hive.event

import java.io.{BufferedReader, InputStreamReader}
import java.net.InetAddress

import org.apache.commons.lang3.{JavaVersion, StringUtils, SystemUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.engine.hive.events.{HiveEngineEvent, HiveOperationEvent, HiveSessionEvent}
import org.apache.kyuubi.events.JsonProtocol
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.ServiceState

class HiveEventLoggingServiceSuite extends HiveJDBCTestHelper {

  private val kyuubiConf = new KyuubiConf()
  private val logRoot = kyuubiConf.get(ENGINE_EVENT_JSON_LOG_PATH)
  private val hostName = InetAddress.getLocalHost.getCanonicalHostName

  // Read every day partition so the test does not depend on the date the suite was constructed,
  // which can differ from the date the events are written if a run crosses midnight.
  private def readEventLines(eventType: String): Seq[String] = {
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val eventFiles = Option(fileSystem.globStatus(
      new Path(logRoot, s"$eventType/day=*/Hive-$hostName.json"))).toSeq.flatten
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

  override def beforeAll(): Unit = {
    HiveSQLEngine.startEngine()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val logPath = new Path(logRoot)
    if (fileSystem.exists(logPath)) {
      fileSystem.delete(logPath, true)
    }
    fileSystem.close()
  }

  override protected def jdbcUrl: String = {
    "jdbc:hive2://" + HiveSQLEngine.currentEngine.get.frontendServices.head.connectionUrl + "/;"
  }

  test("test engine event logging") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))

    val engineEvents = readEventLines("hive_engine")
    val initializedEvent =
      JsonProtocol.jsonToEvent(engineEvents.head, classOf[HiveEngineEvent])
    assert(initializedEvent.asInstanceOf[HiveEngineEvent].state.equals(ServiceState.INITIALIZED))

    val startedEvent =
      JsonProtocol.jsonToEvent(engineEvents(1), classOf[HiveEngineEvent])
    assert(startedEvent.asInstanceOf[HiveEngineEvent].state.equals(ServiceState.STARTED))
  }

  test("test session event logging") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))

    withJdbcStatement() { statement =>
      val catalogs = statement.getConnection.getMetaData.getCatalogs
      assert(!catalogs.next())
      val sessionEvents = readEventLines("hive_session")
      val readEvent =
        JsonProtocol.jsonToEvent(sessionEvents.head, classOf[HiveSessionEvent])
      assert(readEvent.isInstanceOf[HiveSessionEvent])
    }
  }

  test("test operation event logging") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))

    withJdbcStatement("hive_engine_test") { statement =>
      val createTableStatement = "CREATE TABLE hive_engine_test(id int, value string) stored as orc"
      statement.execute(createTableStatement)
      val insertStatement = "INSERT INTO hive_engine_test SELECT 1, '2'"
      statement.execute(insertStatement)
      val queryStatement = "SELECT ID, VALUE FROM hive_engine_test"
      statement.executeQuery(queryStatement)

      val operationEvents = readEventLines("hive_operation")

      val createTableEvent =
        JsonProtocol.jsonToEvent(operationEvents.head, classOf[HiveOperationEvent])
      assert(createTableEvent.isInstanceOf[HiveOperationEvent])
      assert(StringUtils.equals(
        createTableEvent.asInstanceOf[HiveOperationEvent].statement,
        createTableStatement))

      val insertEvent =
        JsonProtocol.jsonToEvent(operationEvents(1), classOf[HiveOperationEvent])
      assert(insertEvent.isInstanceOf[HiveOperationEvent])
      assert(StringUtils.equals(
        insertEvent.asInstanceOf[HiveOperationEvent].statement,
        insertStatement))

      val queryEvent =
        JsonProtocol.jsonToEvent(operationEvents(2), classOf[HiveOperationEvent])
      assert(queryEvent.isInstanceOf[HiveOperationEvent])
      assert(StringUtils.equals(
        queryEvent.asInstanceOf[HiveOperationEvent].statement,
        queryStatement))
    }
  }
}
