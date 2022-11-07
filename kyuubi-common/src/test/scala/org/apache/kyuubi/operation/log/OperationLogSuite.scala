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

package org.apache.kyuubi.operation.log

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TProtocolVersion, TRowSet}

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.session.NoopSessionManager
import org.apache.kyuubi.util.ThriftUtils

class OperationLogSuite extends KyuubiFunSuite {

  val msg1 = "This is just a dummy log message 1"
  val msg2 = "This is just a dummy log message 2"

  test("create, delete, read and write to server operation log") {
    val sessionManager = new NoopSessionManager
    sessionManager.initialize(KyuubiConf())
    val sHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      "kyuubi",
      "passwd",
      "localhost",
      Map.empty)
    val session = sessionManager.getSession(sHandle)
    val oHandle = OperationHandle()
    assert(sessionManager.operationLogRoot.isDefined)
    val operationLogRoot = sessionManager.operationLogRoot.get

    OperationLog.createOperationLogRootDirectory(session)
    assert(Files.exists(Paths.get(operationLogRoot, sHandle.identifier.toString)))
    assert(Files.isDirectory(Paths.get(operationLogRoot, sHandle.identifier.toString)))

    val operationLog = OperationLog.createOperationLog(session, oHandle)
    val logFile =
      Paths.get(operationLogRoot, sHandle.identifier.toString, oHandle.identifier.toString)
    // lazy create
    assert(!Files.exists(logFile))

    OperationLog.setCurrentOperationLog(operationLog)
    assert(OperationLog.getCurrentOperationLog === operationLog)

    OperationLog.removeCurrentOperationLog()
    assert(OperationLog.getCurrentOperationLog === null)

    operationLog.write(msg1 + "\n")
    assert(Files.exists(logFile))

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
    assert(Files.exists(Paths.get(operationLogRoot, sHandle.identifier.toString)))
    sessionManager.closeSession(sHandle)
    assert(!Files.exists(Paths.get(operationLogRoot, sHandle.identifier.toString)))
  }

  test("log divert appender") {
    val sessionManager = new NoopSessionManager
    sessionManager.initialize(KyuubiConf())
    val sHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      "kyuubi",
      "passwd",
      "localhost",
      Map.empty)
    val session = sessionManager.getSession(sHandle)
    val oHandle = OperationHandle()

    OperationLog.createOperationLogRootDirectory(session)
    val operationLog = OperationLog.createOperationLog(session, oHandle)
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

  test("exception when creating log files") {
    val sessionManager = new NoopSessionManager
    sessionManager.initialize(KyuubiConf())
    val sHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      "kyuubi",
      "passwd",
      "localhost",
      Map.empty)
    val session = sessionManager.getSession(sHandle)
    assert(sessionManager.operationLogRoot.isDefined)
    val operationLogRoot = sessionManager.operationLogRoot.get

    val logRoot = Paths.get(operationLogRoot, sHandle.identifier.toString).toFile
    logRoot.deleteOnExit()
    Files.createFile(Paths.get(operationLogRoot, sHandle.identifier.toString))
    assert(logRoot.exists())
    OperationLog.createOperationLogRootDirectory(session)
    assert(logRoot.isFile)
    val oHandle = OperationHandle()

    val log = OperationLog.createOperationLog(session, oHandle)
    val tRowSet = log.read(1)
    assert(tRowSet == ThriftUtils.newEmptyRowSet)

    logRoot.delete()

    OperationLog.createOperationLogRootDirectory(session)
    val log1 = OperationLog.createOperationLog(session, oHandle)
    log1.write("some msg here \n")
    log1.close()
    log1.write("some msg here again")
    val e = intercept[KyuubiSQLException](log1.read(-1))
    assert(e.getMessage.contains(s"${sHandle.identifier}/${oHandle.identifier}"))
  }

  test("test fail to init operation log root dir") {
    val sessionManager = new NoopSessionManager
    val tempDir = Utils.createTempDir().toFile
    tempDir.setExecutable(false)

    val conf = KyuubiConf()
      .set(KyuubiConf.SERVER_OPERATION_LOG_DIR_ROOT, tempDir.getAbsolutePath + "/operation_logs")
    sessionManager.initialize(conf)
    assert(sessionManager.operationLogRoot.isEmpty)

    tempDir.setExecutable(true)
    tempDir.delete()
  }

  test("test support extra readers") {
    val sessionManager = new NoopSessionManager
    sessionManager.initialize(KyuubiConf())
    val sHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      "kyuubi",
      "passwd",
      "localhost",
      Map.empty)
    val session = sessionManager.getSession(sHandle)
    val oHandle = OperationHandle()
    OperationLog.createOperationLogRootDirectory(session)

    val operationLog = OperationLog.createOperationLog(session, oHandle)
    val tempDir = Utils.createTempDir()
    val extraLog = new File(tempDir.toFile, "extra.log").toPath
    extraLog.toFile.createNewFile()

    operationLog.write(msg1)
    Files.write(extraLog, msg2.getBytes)
    operationLog.addExtraLog(extraLog)

    val rowSet = operationLog.read(-1).getColumns.get(0).getStringVal.getValues
    assert(rowSet.get(0) == msg1)
    assert(rowSet.get(1) == msg2)

    operationLog.close()
    tempDir.toFile.delete()
  }

  test("test seek reader") {
    val file = Utils.createTempDir().resolve("f")
    val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
    try {
      0.until(10).foreach(x => writer.write(s"$x\n"))
      writer.flush()
      writer.close()

      def compareResult(rows: TRowSet, expected: Seq[String]): Unit = {
        val res = rows.getColumns.get(0).getStringVal.getValues.asScala
        assert(res.size == expected.size)
        res.zip(expected).foreach { case (l, r) =>
          assert(l == r)
        }
      }

      val log = new OperationLog(file)
      // The operation log file is created externally and should be initialized actively.
      log.initOperationLogIfNecessary()

      compareResult(log.read(-1, 1), Seq("0"))
      compareResult(log.read(-1, 1), Seq("1"))
      compareResult(log.read(0, 1), Seq("0"))
      compareResult(log.read(0, 2), Seq("0", "1"))
      compareResult(log.read(5, 10), Seq("5", "6", "7", "8", "9"))
    } finally {
      Utils.deleteDirectoryRecursively(file.toFile)
    }
  }

  test("[KYUUBI #3511] Reading an uninitialized log should return empty rowSet") {
    val sessionManager = new NoopSessionManager
    sessionManager.initialize(KyuubiConf())
    val sHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      "kyuubi",
      "passwd",
      "localhost",
      Map.empty)
    val session = sessionManager.getSession(sHandle)
    val oHandle = OperationHandle()

    val log = OperationLog.createOperationLog(session, oHandle)
    // It has not been initialized, and returns empty `TRowSet` directly.
    val tRowSet = log.read(1)
    assert(tRowSet == ThriftUtils.newEmptyRowSet)

    OperationLog.createOperationLogRootDirectory(session)
    val log1 = OperationLog.createOperationLog(session, oHandle)
    // write means initialized operationLog, we can read log directly later
    log1.write(msg1)
    val msg = log1.read(1).getColumns.get(0).getStringVal.getValues.asScala.head
    assert(msg == msg1)
  }

  test("closing existing seekable reader when adding extra log") {
    val file = Utils.createTempDir().resolve("f")
    val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
    val extraFile = Utils.createTempDir().resolve("e")
    val extraWriter = Files.newBufferedWriter(extraFile, StandardCharsets.UTF_8)

    try {
      writer.write(s"log")
      writer.flush()
      writer.close()

      extraWriter.write("extra_log")
      extraWriter.flush()
      extraWriter.close()

      def compareResult(rows: TRowSet, expected: Seq[String]): Unit = {
        val res = rows.getColumns.get(0).getStringVal.getValues.asScala
        assert(res.size == expected.size)
        res.zip(expected).foreach { case (l, r) =>
          assert(l == r)
        }
      }

      val log = new OperationLog(file)
      // The operation log file is created externally and should be initialized actively.
      log.initOperationLogIfNecessary()

      compareResult(log.read(0, 1), Seq("log"))
      log.addExtraLog(extraFile)
      compareResult(log.read(1, 1), Seq("extra_log"))
    } finally {
      Utils.deleteDirectoryRecursively(file.toFile)
      Utils.deleteDirectoryRecursively(extraFile.toFile)
    }
  }
}
