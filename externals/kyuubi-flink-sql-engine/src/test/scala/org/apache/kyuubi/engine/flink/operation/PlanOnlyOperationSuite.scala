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

import java.sql.Statement
import java.util.UUID

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.{WithDiscoveryFlinkSQLEngine, WithFlinkSQLEngineLocal}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_NAMESPACE}
import org.apache.kyuubi.operation.{AnalyzeMode, ExecutionMode, HiveJDBCTestHelper, ParseMode, PhysicalMode}
import org.apache.kyuubi.util.AssertionUtils._

class PlanOnlyOperationSuite extends WithFlinkSQLEngineLocal
  with HiveJDBCTestHelper with WithDiscoveryFlinkSQLEngine {

  override protected def engineRefId: String = UUID.randomUUID().toString

  override protected def namespace: String = "/kyuubi/flink-plan-only-test"

  def engineType: String = "flink"

  override def withKyuubiConf: Map[String, String] =
    Map(
      "flink.execution.target" -> "remote",
      HA_NAMESPACE.key -> namespace,
      HA_ENGINE_REF_ID.key -> engineRefId,
      KyuubiConf.ENGINE_TYPE.key -> "FLINK_SQL",
      KyuubiConf.ENGINE_SHARE_LEVEL.key -> "user",
      KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> ParseMode.name,
      KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "plan-only") ++ testExtraConf

  override protected def jdbcUrl: String = getFlinkEngineServiceUrl

  test("Plan only operation with system defaults") {
    withJdbcStatement() { statement =>
      testPlanOnlyStatementWithParseMode(statement)
    }
  }

  test("Plan only operation with session conf") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> AnalyzeMode.name))(Map.empty) {
      withJdbcStatement() { statement =>
        interceptContains[Exception](statement.executeQuery("select 1"))(
          s"The operation mode ${AnalyzeMode.name} doesn't support in Flink SQL engine.")
      }
    }
  }

  test("Plan only operation with set command") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> AnalyzeMode.name))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.execute(s"set ${KyuubiConf.OPERATION_PLAN_ONLY_MODE.key}=${ParseMode.name}")
        testPlanOnlyStatementWithParseMode(statement)
      }
    }
  }

  test("Plan only operation with PHYSICAL mode") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> PhysicalMode.name))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("Calc(select=[1 AS EXPR$0])") &&
          operationPlan.contains("Values(type=[RecordType(INTEGER ZERO)], tuples=[[{ 0 }]])"))
      }
    }
  }

  test("Plan only operation with EXECUTION mode") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> ExecutionMode.name))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("Calc(select=[1 AS EXPR$0])") &&
          operationPlan.contains("Values(tuples=[[{ 0 }]])"))
      }
    }
  }

  private def testPlanOnlyStatementWithParseMode(statement: Statement): Unit = {
    val operationPlan = getOperationPlanWithStatement(statement)
    assert(operationPlan.startsWith("LogicalProject(EXPR$0=[1])") &&
      operationPlan.contains("LogicalValues(tuples=[[{ 0 }]])"))
  }

  private def getOperationPlanWithStatement(statement: Statement): String = {
    val resultSet = statement.executeQuery("select 1")
    assert(resultSet.next())
    resultSet.getString(1)
  }
}
