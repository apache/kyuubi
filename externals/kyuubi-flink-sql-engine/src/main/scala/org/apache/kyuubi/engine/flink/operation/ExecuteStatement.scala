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

import java.util.Optional

import scala.concurrent.duration.Duration

import org.apache.flink.api.common.JobID
import org.apache.flink.table.api.TableException
import org.apache.flink.table.gateway.api.operation.OperationHandle
import org.apache.flink.table.operations.Operation
import org.apache.flink.table.operations.command.HelpOperation

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.flink.FlinkEngineUtils
import org.apache.kyuubi.engine.flink.result.ResultSetUtil
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.reflect.{DynConstructors, DynFields, DynMethods}

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    resultMaxRows: Int,
    resultFetchTimeout: Duration)
  extends FlinkOperation(session) with Logging {

  private val operationLog: OperationLog =
    OperationLog.createOperationLog(session, getHandle)

  var jobId: Option[JobID] = None

  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    executeStatement()
  }

  private def executeStatement(): Unit = {
    try {
      setState(OperationState.RUNNING)

      val operation = parseExtendedStatement(statement)
      if (operation.isPresent && operation.get().isInstanceOf[HelpOperation]) {
        resultSet = ResultSetUtil.helpMessageResultSet
        setState(OperationState.FINISHED)
        return
      }

      val resultFetcher = executor.executeStatement(
        new OperationHandle(getHandle.identifier),
        statement)
      jobId = FlinkEngineUtils.getResultJobId(resultFetcher)
      resultSet = ResultSetUtil.fromResultFetcher(resultFetcher, resultMaxRows, resultFetchTimeout)
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      shutdownTimeoutMonitor()
    }
  }

  private def parseExtendedStatement(statement: String): Optional[Operation] = {
    val plannerModuleClassLoader: ClassLoader = getPlannerModuleClassLoader
    val extendedParser: AnyRef =
      DynConstructors.builder()
        .loader(plannerModuleClassLoader)
        .impl("org.apache.flink.table.planner.parse.ExtendedParser")
        .build().newInstance()
    DynMethods.builder("parse")
      .hiddenImpl(extendedParser.getClass, classOf[String])
      .buildChecked()
      .invoke(extendedParser, statement)
  }

  private def getPlannerModuleClassLoader: ClassLoader = {
    try {
      val plannerModule = DynMethods.builder("getInstance")
        .hiddenImpl("org.apache.flink.table.planner.loader.PlannerModule")
        .buildStaticChecked()
        .invoke().asInstanceOf[AnyRef]

      DynFields.builder()
        .hiddenImpl(plannerModule.getClass, "submoduleClassLoader")
        .build[ClassLoader].bind(plannerModule).get
    } catch {
      case e: Exception =>
        throw new TableException(
          "Error obtaining Flink planner module ClassLoader. " +
            "Make sure a flink-table-planner-loader.jar is on the classpath",
          e)
    }
  }
}
