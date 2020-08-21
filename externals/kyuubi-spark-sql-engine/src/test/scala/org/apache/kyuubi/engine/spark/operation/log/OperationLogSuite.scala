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

package org.apache.kyuubi.engine.spark.operation.log

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.operation.{OperationHandle, OperationType}
import org.apache.kyuubi.session.SessionHandle

class OperationLogSuite extends KyuubiFunSuite {

  val msg1 = "This is just a dummy log message 1"
  val msg2 = "This is just a dummy log message 2"

  test("create, delete, read and write to operation log") {
    val sHandle = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    val oHandle = OperationHandle(
      OperationType.EXECUTE_STATEMENT, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)

    OperationLog.createOperationLogRootDirectory(sHandle)
    assert(Files.exists(Paths.get(OperationLog.LOG_ROOT, sHandle.identifier.toString)))
    assert(Files.isDirectory(Paths.get(OperationLog.LOG_ROOT, sHandle.identifier.toString)))

    val operationLog = OperationLog.createOperationLog(sHandle, oHandle)
    val logFile =
      Paths.get(OperationLog.LOG_ROOT, sHandle.identifier.toString, oHandle.identifier.toString)
    assert(Files.exists(logFile))

    OperationLog.setCurrentOperationLog(operationLog)
    assert(OperationLog.getCurrentOperationLog === operationLog)

    OperationLog.removeCurrentOperationLog()
    assert(OperationLog.getCurrentOperationLog === null)

    operationLog.write(msg1 + "\n")
    val tRowSet1 = operationLog.read(1)
    assert(tRowSet1.getColumns.get(0).getStringVal.getValues.get(0) === msg1)
    val tRowSet2 = operationLog.read(1)
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.isEmpty)

    operationLog.write(msg1 + "\n")
    operationLog.write(msg2 + "\n")
    val tRowSet3 = operationLog.read(-1).getColumns.get(0).getStringVal.getValues
    assert(tRowSet3.get(0) === msg1)
    assert(tRowSet3.get(1) === msg2)

    operationLog.close()
    assert(!Files.exists(logFile))
  }

  test("log divert appender") {
    val sHandle = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    val oHandle = OperationHandle(
      OperationType.EXECUTE_STATEMENT, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)

    OperationLog.createOperationLogRootDirectory(sHandle)
    val operationLog = OperationLog.createOperationLog(sHandle, oHandle)
    OperationLog.setCurrentOperationLog(operationLog)

    LogDivertAppender.initialize()

    info(msg1)
    info(msg2)
    warn(msg2)
    error(msg2)

    val list =
      operationLog.read(-1).getColumns.get(0).getStringVal.getValues.asScala.distinct
    val logMsg1 = list(0)
    assert(logMsg1.contains("INFO") &&
      logMsg1.contains(classOf[OperationLogSuite].getSimpleName) && logMsg1.endsWith(msg1))
    val logMsg2 = list(2)
    assert(logMsg2.contains("WARN") && logMsg2.endsWith(msg2))
    val logMsg3 = list(3)
    assert(logMsg3.contains("ERROR") && logMsg3.endsWith(msg2))

    OperationLog.removeCurrentOperationLog()
    info(msg1)
    val list2 = operationLog.read(-1).getColumns.get(0).getStringVal.getValues
    assert(list2.isEmpty)
    operationLog.close()
  }
}
