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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.OperationModes._
import org.apache.kyuubi.engine.flink.WithFlinkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class PlanOnlyOperationSuite extends WithFlinkSQLEngine with HiveJDBCTestHelper {

  override def withKyuubiConf: Map[String, String] =
    Map(
      KyuubiConf.ENGINE_SHARE_LEVEL.key -> "user",
      KyuubiConf.OPERATION_PLAN_ONLY.key -> PARSE.toString,
      KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "plan-only")

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;"

  test("Plan only operation with system defaults") {
    withJdbcStatement() { statement =>
      testPlanOnlyStatement(statement)
    }
  }

  test("Plan only operation with session conf") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY.key -> ANALYZE.toString))(Map.empty) {
      withJdbcStatement() { statement =>
        val exceptionMsg = intercept[Exception](statement.executeQuery("select 1")).getMessage
        assert(exceptionMsg.contains(
          s"The operation mode ${ANALYZE.toString} doesn't support in Flink SQL engine."))
      }
    }
  }

  test("Plan only operation with set command") {
    withSessionConf()(Map(KyuubiConf.OPERATION_PLAN_ONLY.key -> ANALYZE.toString))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.execute(s"set ${KyuubiConf.OPERATION_PLAN_ONLY.key}=parse")
        testPlanOnlyStatement(statement)
      }
    }
  }

  private def testPlanOnlyStatement(statement: Statement): Unit = {
    val resultSet = statement.executeQuery("select 1")
    assert(resultSet.next())
    assert(resultSet.getString(1) === "LogicalProject(EXPR$0=[1])")
  }
}
