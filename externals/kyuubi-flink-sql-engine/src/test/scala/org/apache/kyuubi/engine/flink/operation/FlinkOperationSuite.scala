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

import java.net.URL
import java.util
import java.util.Collections

import org.apache.flink.client.cli.DefaultCLI
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{ConfigConstants, Configuration, MemorySize, TaskManagerOptions, WebOptions}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.table.client.gateway.context.{DefaultContext, SessionContext}
import org.apache.flink.table.client.gateway.local.LocalExecutor
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.flink.session.{FlinkSessionImpl, FlinkSQLSessionManager}
import org.apache.kyuubi.operation.FetchOrientation

class FlinkOperationSuite extends KyuubiFunSuite {

  val user: String = Utils.currentUser
  val password = "anonymous"

  val NUM_TMS = 2
  val NUM_SLOTS_PER_TM = 2

  private def getConfig = {
    val config = new Configuration
    config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"))
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS)
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM)
    config.setBoolean(WebOptions.SUBMIT_ENABLE, false)
    config
  }

  val MINI_CLUSTER_RESOURCE =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(getConfig)
        .setNumberTaskManagers(NUM_TMS)
        .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM).build)

  var clusterClient: ClusterClient[_] = _

  var engineContext = new DefaultContext(
    Collections.emptyList(),
    new Configuration,
    Collections.singletonList(new DefaultCLI))
  var sessionContext: SessionContext = _
  var flinkSession: FlinkSessionImpl = _

  private def createLocalExecutor: LocalExecutor =
    createLocalExecutor(Collections.emptyList[URL], new Configuration)

  private def createLocalExecutor(
      dependencies: util.List[URL],
      configuration: Configuration): LocalExecutor = {
    configuration.addAll(clusterClient.getFlinkConfiguration)
    val defaultContext: DefaultContext = new DefaultContext(
      dependencies,
      configuration,
      Collections.singletonList(new DefaultCLI))
    new LocalExecutor(defaultContext)
  }

  override def beforeAll(): Unit = {
    MINI_CLUSTER_RESOURCE.before()
    clusterClient = MINI_CLUSTER_RESOURCE.getClusterClient

    sessionContext = SessionContext.create(engineContext, "test-session-id");
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
    val getCatalogOperation = new GetCatalogs(flinkSession)
    getCatalogOperation.run()

    val resultSet = getCatalogOperation.getNextRowSet(FetchOrientation.FETCH_FIRST, 10)
    assert(1 == resultSet.getRowsSize)
    assert(resultSet.getRows.get(0).getColVals().get(0).getStringVal.getValue === "default_catalog")
  }

  test("execute statement -  select column name with dots") {
    val executeStatementOp = new ExecuteStatement(flinkSession, "select 'tmp.hello'", false, -1)
    val executor = createLocalExecutor
    executor.openSession("test-session")
    executeStatementOp.setExecutor(executor)
    executeStatementOp.setSessionId("test-session")
    executeStatementOp.run()

    val resultSet = executeStatementOp.getNextRowSet(FetchOrientation.FETCH_FIRST, 10)
    assert(1 == resultSet.getRowsSize)
    assert(resultSet.getRows.get(0).getColVals().get(0).getStringVal.getValue === "tmp.hello")
  }

}
