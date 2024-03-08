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

package org.apache.kyuubi.engine.flink.operation

import java.util.UUID

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.ShareLevel.ShareLevel
import org.apache.kyuubi.engine.flink.{WithDiscoveryFlinkSQLEngine, WithFlinkSQLEngineLocal}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_NAMESPACE}
import org.apache.kyuubi.operation.{HiveJDBCTestHelper, NoneMode}

trait FlinkEngineInitializeSuite extends HiveJDBCTestHelper
  with WithDiscoveryFlinkSQLEngine with WithFlinkSQLEngineLocal {

  protected def jdbcUrl: String = getFlinkEngineServiceUrl

  protected val ENGINE_INITIALIZE_SQL_VALUE: String =
    "show databases;"

  protected val ENGINE_SESSION_INITIALIZE_SQL_VALUE: String =
    """create catalog cat_b with ('type'='generic_in_memory');
      |create table blackhole(i int) with ('connector'='blackhole');
      |create table datagen(i int) with (
      |'connector'='datagen',
      |'fields.i.kind'='sequence',
      |'fields.i.start'='1',
      |'fields.i.end'='10')""".stripMargin

  override def withKyuubiConf: Map[String, String] = {
    Map(
      "flink.execution.target" -> "remote",
      "flink.high-availability.cluster-id" -> "flink-mini-cluster",
      "flink.app.name" -> "kyuubi_connection_flink_kandy",
      HA_NAMESPACE.key -> namespace,
      HA_ENGINE_REF_ID.key -> engineRefId,
      ENGINE_TYPE.key -> "FLINK_SQL",
      ENGINE_SHARE_LEVEL.key -> shareLevel.toString,
      OPERATION_PLAN_ONLY_MODE.key -> NoneMode.name,
      ENGINE_FLINK_INITIALIZE_SQL.key -> ENGINE_INITIALIZE_SQL_VALUE,
      ENGINE_SESSION_FLINK_INITIALIZE_SQL.key -> ENGINE_SESSION_INITIALIZE_SQL_VALUE,
      KYUUBI_SESSION_USER_KEY -> "kandy")
  }

  override protected def engineRefId: String = UUID.randomUUID().toString

  def namespace: String = "/kyuubi/flink-local-engine-test"

  def shareLevel: ShareLevel

  def engineType: String = "flink"

  test("execute statement - kyuubi engine initialize") {
    withJdbcStatement() { statement =>
      var resultSet = statement.executeQuery("show catalogs")
      val expectedCatalogs = Set("default_catalog", "cat_b")
      var actualCatalogs = Set[String]()
      while (resultSet.next()) {
        actualCatalogs += resultSet.getString(1)
      }
      assert(expectedCatalogs.subsetOf(actualCatalogs))

      resultSet = statement.executeQuery("show databases")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "default_database")
      assert(!resultSet.next())

      val expectedTables = Set("blackhole", "datagen")
      resultSet = statement.executeQuery("show tables")
      while (resultSet.next()) {
        assert(expectedTables.contains(resultSet.getString(1)))
      }
      assert(!resultSet.next())

      var dropResult = statement.executeQuery("drop catalog cat_b")
      assert(dropResult.next())
      assert(dropResult.getString(1) === "OK")

      dropResult = statement.executeQuery("drop table blackhole")
      assert(dropResult.next())
      assert(dropResult.getString(1) === "OK")

      dropResult = statement.executeQuery("drop table datagen")
      assert(dropResult.next())
      assert(dropResult.getString(1) === "OK")
    }
    // check engine alive status after close session with connection level engine
    if (shareLevel == ShareLevel.CONNECTION) {
      eventually(Timeout(10.seconds)) {
        assert(!engineProcess.isAlive)
      }
      val e = intercept[Exception] {
        withJdbcStatement() { statement =>
          statement.executeQuery("select 1")
        }
      }
      assert(e.getMessage() == "Time out retrieving Flink engine service url.")
    }
    // check engine alive status after close session with user level engine
    if (shareLevel == ShareLevel.USER) {
      assert(engineProcess.isAlive)
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery("select 1")
        assert(resultSet.next())
      }
    }
  }
}

class FlinkConnectionLevelEngineInitializeSuite extends FlinkEngineInitializeSuite {
  def shareLevel: ShareLevel = ShareLevel.CONNECTION
}

class FlinkUserLevelEngineInitializeSuite extends FlinkEngineInitializeSuite {
  def shareLevel: ShareLevel = ShareLevel.USER
}
