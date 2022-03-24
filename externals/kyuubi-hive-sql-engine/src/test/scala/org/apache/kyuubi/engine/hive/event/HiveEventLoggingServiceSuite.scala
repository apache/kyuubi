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

import java.nio.file.Paths

import org.apache.hive.service.rpc.thrift.TExecuteStatementReq

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.operation.{HiveJDBCTestHelper, OperationHandle}

class HiveEventLoggingServiceSuite extends HiveJDBCTestHelper {

  private val logRoot = "file://" + Utils.createTempDir().toString
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())

  override def beforeAll(): Unit = {
    HiveSQLEngine.startEngine()
    super.beforeAll()
  }

  override protected def jdbcUrl: String = {
    "jdbc:hive2://" + HiveSQLEngine.currentEngine.get.frontendServices.head.connectionUrl + "/;"
  }

  test("statementEvent: generate, dump and query") {
    val statementEventPath = Paths.get(
      logRoot,
      "spark_operation",
      s"day=$currentDate",
      "hive-event-log.json")
    val sql = "select timestamp'2021-09-01';"
    withSessionHandle { (client, handle) =>
      val table = statementEventPath.getParent
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val opHandle = tExecuteStatementResp.getOperationHandle
      val statementId = OperationHandle(opHandle).identifier.toString

    }
  }

}
