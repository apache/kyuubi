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

import java.util.Collections

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.flink.client.cli.DefaultCLI
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader
import org.apache.flink.configuration.Configuration
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.config.EngineEnvironment
import org.apache.kyuubi.engine.flink.config.entries.{ExecutionEntry, ViewEntry}
import org.apache.kyuubi.engine.flink.context.{EngineContext, SessionContext}
import org.apache.kyuubi.engine.flink.session.{FlinkSessionImpl, FlinkSQLSessionManager}
import org.apache.kyuubi.operation.FetchOrientation

class FlinkOperationSuite extends KyuubiFunSuite {

  val user: String = Utils.currentUser
  val password = "anonymous"

  var engineEnv = new EngineEnvironment
  var engineContext = new EngineContext(
    engineEnv,
    Collections.emptyList(),
    new Configuration,
    new DefaultCLI,
    new DefaultClusterClientServiceLoader)
  var sessionContext: SessionContext = _
  var flinkSession: FlinkSessionImpl = _

  override def beforeAll(): Unit = {
    engineEnv = EngineEnvironment.enrich(
      engineContext.getEngineEnv,
      Map(EngineEnvironment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_TYPE
        -> ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH).asJava,
      Collections.emptyMap[String, ViewEntry])
    sessionContext = new SessionContext(engineEnv, engineContext)
    val flinkSQLSessionManager = new FlinkSQLSessionManager(engineContext)
    flinkSQLSessionManager.initialize(KyuubiConf())
    flinkSession = new FlinkSessionImpl(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
      user,
      password,
      "localhost",
      Map(),
      flinkSQLSessionManager,
      sessionContext)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("get catalogs for flink sql") {
    val getCatalogOperation = new GetCatalogs(sessionContext, flinkSession)
    getCatalogOperation.run()

    val resultSet = getCatalogOperation.getNextRowSet(FetchOrientation.FETCH_FIRST, 10)
    assert(1 == resultSet.getRowsSize)
    assert(resultSet.getRows.get(0).getColVals().get(0).getStringVal.getValue === "default_catalog")
  }

}
