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

import java.sql.Statement

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.OperationModes._

class PlanOnlyOperationSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.ENGINE_SHARE_LEVEL, "user")
      .set(KyuubiConf.OPERATION_PLAN_ONLY_MODE, OPTIMIZE.toString)
      .set(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "plan-only")
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("KYUUBI #1059: Plan only operation with system defaults") {
    withJdbcStatement() { statement =>
      val operationPlan = getOperationPlanWithStatement(statement)
      assert(operationPlan.startsWith("Project") && !operationPlan.contains("Filter"))
    }
  }

  test("KYUUBI #1059: Plan only operation with session conf") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> ANALYZE.toString))(Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("Project") && operationPlan.contains("Filter"))
      }
    }
  }

  test("KYUUBI #1059: Plan only operation with set command") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> ANALYZE.toString))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.execute(s"set ${KyuubiConf.OPERATION_PLAN_ONLY_MODE.key}=$PARSE")
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("'Project"))
      }
    }
  }

  test("KYUUBI #1919: Plan only operation with PHYSICAL mode") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> PHYSICAL.toString))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("Project") && operationPlan.contains("Scan OneRowRelation"))
      }
    }
  }

  test("KYUUBI #1919: Plan only operation with EXECUTION mode") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> EXECUTION.toString))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val operationPlan = getOperationPlanWithStatement(statement)
        assert(operationPlan.startsWith("*(1) Project") &&
          operationPlan.contains("*(1) Scan OneRowRelation"))
      }
    }
  }

  test("KYUUBI #1920: Plan only operations with UseStatement or SetNamespaceCommand") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> NONE.toString))(Map.empty) {
      withDatabases("test_database") { statement =>
        statement.execute("create database test_database")
        statement.execute(s"set ${KyuubiConf.OPERATION_PLAN_ONLY_MODE.key}=$OPTIMIZE")
        val result = statement.executeQuery("use test_database")
        assert(!result.next(), "In contrast to PlanOnly mode, it will returns an empty result")
      }
    }
  }

  test("KYUUBI #1920: Plan only operations with CreateViewStatement or CreateViewCommand") {
    withSessionConf()(
      Map(KyuubiConf.OPERATION_PLAN_ONLY_EXCLUDES.key -> "CreateViewStatement,CreateViewCommand"))(
      Map.empty) {
      withJdbcStatement() { statement =>
        val result = statement.executeQuery("create temp view temp_view as select 1")
        assert(!result.next(), "In contrast to PlanOnly mode, it will returns an empty result")
        statement.execute("drop view temp_view")
      }
    }
  }

  private def getOperationPlanWithStatement(statement: Statement): String = {
    val resultSet = statement.executeQuery("select 1 where true")
    assert(resultSet.next())
    resultSet.getString(1)
  }
}
