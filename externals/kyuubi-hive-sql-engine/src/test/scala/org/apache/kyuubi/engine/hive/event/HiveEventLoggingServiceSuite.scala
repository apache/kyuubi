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
import java.nio.file.Paths

import org.apache.commons.lang3.{JavaVersion, StringUtils, SystemUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.kyuubi.Utils
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
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())
  private val hostName = InetAddress.getLocalHost.getCanonicalHostName

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

    val engineEventPath = Paths.get(
      logRoot,
      "hive_engine",
      s"day=$currentDate",
      s"Hive-$hostName.json")
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val fs: FSDataInputStream = fileSystem.open(new Path(engineEventPath.toString))
    val engineEventReader = new BufferedReader(new InputStreamReader(fs))
    val initializedEvent =
      JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[HiveEngineEvent])
    assert(initializedEvent.asInstanceOf[HiveEngineEvent].state.equals(ServiceState.INITIALIZED))

    val startedEvent =
      JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[HiveEngineEvent])
    assert(startedEvent.asInstanceOf[HiveEngineEvent].state.equals(ServiceState.STARTED))
  }

  test("test session event logging") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))

    withJdbcStatement() { statement =>
      val catalogs = statement.getConnection.getMetaData.getCatalogs
      assert(!catalogs.next())
      val sessionEventPath = Paths.get(
        logRoot,
        "hive_session",
        s"day=$currentDate",
        s"Hive-$hostName.json")
      val fileSystem: FileSystem = FileSystem.get(new Configuration())
      val fs: FSDataInputStream = fileSystem.open(new Path(sessionEventPath.toString))
      val engineEventReader = new BufferedReader(new InputStreamReader(fs))
      val readEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[HiveSessionEvent])
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

      val operationEventPath = Paths.get(
        logRoot,
        "hive_operation",
        s"day=$currentDate",
        s"Hive-$hostName.json")

      val fileSystem: FileSystem = FileSystem.get(new Configuration())
      val fs: FSDataInputStream = fileSystem.open(new Path(operationEventPath.toString))
      val engineEventReader = new BufferedReader(new InputStreamReader(fs))

      val createTableEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[HiveOperationEvent])
      assert(createTableEvent.isInstanceOf[HiveOperationEvent])
      assert(StringUtils.equals(
        createTableEvent.asInstanceOf[HiveOperationEvent].statement,
        createTableStatement))

      val insertEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[HiveOperationEvent])
      assert(insertEvent.isInstanceOf[HiveOperationEvent])
      assert(StringUtils.equals(
        insertEvent.asInstanceOf[HiveOperationEvent].statement,
        insertStatement))

      val queryEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[HiveOperationEvent])
      assert(queryEvent.isInstanceOf[HiveOperationEvent])
      assert(StringUtils.equals(
        queryEvent.asInstanceOf[HiveOperationEvent].statement,
        queryStatement))
    }
  }
}
