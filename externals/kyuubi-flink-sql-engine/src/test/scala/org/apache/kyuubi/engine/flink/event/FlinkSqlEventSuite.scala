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
package org.apache.kyuubi.engine.flink.event

import java.io.{BufferedReader, InputStreamReader}
import java.net.InetAddress
import java.nio.file.Paths

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH
import org.apache.kyuubi.config.KyuubiConf.OperationModes.NONE
import org.apache.kyuubi.engine.flink.WithFlinkSQLEngine
import org.apache.kyuubi.events.JsonProtocol
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.ServiceState

class FlinkSqlEventSuite extends WithFlinkSQLEngine with HiveJDBCTestHelper {

  private val logRoot = kyuubiConf.get(ENGINE_EVENT_JSON_LOG_PATH)
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())
  private val hostName = InetAddress.getLocalHost.getCanonicalHostName

  override def withKyuubiConf: Map[String, String] =
    Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> NONE.toString)

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;"

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
      "flink_engine",
      s"day=$currentDate",
      s"Flink-$hostName.json")
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val fs: FSDataInputStream = fileSystem.open(new Path(engineEventPath.toString))
    val engineEventReader = new BufferedReader(new InputStreamReader(fs))

    val startedEvent =
      JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[FlinkEngineEvent])
    assert(startedEvent.asInstanceOf[FlinkEngineEvent].state.equals(ServiceState.STARTED))
  }

  test("test session event logging") {
    withJdbcStatement() { statement =>
      val catalogs = statement.getConnection.getMetaData.getCatalogs
      assert(catalogs.next())
      val sessionEventPath = Paths.get(
        logRoot,
        "flink_session",
        s"day=$currentDate",
        s"Flink-$hostName.json")
      val fileSystem: FileSystem = FileSystem.get(new Configuration())
      val fs: FSDataInputStream = fileSystem.open(new Path(sessionEventPath.toString))
      val engineEventReader = new BufferedReader(new InputStreamReader(fs))
      val readEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[FlinkSessionEvent])
      assert(readEvent.isInstanceOf[FlinkSessionEvent])
    }
  }

  test("test operation event logging") {

    withJdbcStatement() { statement =>
      val selectStatement = "select 1"
      val resultSet = statement.executeQuery(selectStatement)
      assert(resultSet.next())
      assert(resultSet.getString(1) === "1")
      val operationEventPath = Paths.get(
        logRoot,
        "flink_operation",
        s"day=$currentDate",
        s"Flink-$hostName.json")

      val fileSystem: FileSystem = FileSystem.get(new Configuration())
      val fs: FSDataInputStream = fileSystem.open(new Path(operationEventPath.toString))
      val engineEventReader = new BufferedReader(new InputStreamReader(fs))

      val selectEvent =
        JsonProtocol.jsonToEvent(engineEventReader.readLine(), classOf[FlinkOperationEvent])
      assert(selectEvent.isInstanceOf[FlinkOperationEvent])
      assert(StringUtils.equals(
        selectEvent.asInstanceOf[FlinkOperationEvent].statement,
        selectStatement))
    }
  }
}
