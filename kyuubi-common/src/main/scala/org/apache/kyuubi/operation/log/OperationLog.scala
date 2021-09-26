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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.apache.hive.service.rpc.thrift.{TColumn, TRow, TRowSet, TStringColumn}

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_OPERATION_LOG_DIR_ROOT, SERVER_OPERATION_LOG_DIR_ROOT}
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.session.SessionHandle

object OperationLog extends Logging {
  val conf = KyuubiConf()

  def SERVER_LOG_ROOT: String = if (Utils.isTesting) {
    "target/operation_logs"
  } else {
    conf.get(SERVER_OPERATION_LOG_DIR_ROOT).getOrElse("operation_logs")
  }

  def ENGINE_LOG_ROOT: String = if (Utils.isTesting) {
    "target/operation_logs"
  } else {
    conf.get(ENGINE_OPERATION_LOG_DIR_ROOT).getOrElse("operation_logs")
  }

  private final val OPERATION_LOG: InheritableThreadLocal[OperationLog] = {
    new InheritableThreadLocal[OperationLog] {
      override def initialValue(): OperationLog = null
    }
  }

  def setCurrentOperationLog(operationLog: OperationLog): Unit = {
    OPERATION_LOG.set(operationLog)
  }

  def getCurrentOperationLog: OperationLog = OPERATION_LOG.get()

  def removeCurrentOperationLog(): Unit = OPERATION_LOG.remove()

  def createServerOperationLogRootDirectory(sessionHandle: SessionHandle): Unit = {
    createOperationLogRootDirectory(SERVER_LOG_ROOT, sessionHandle)
  }

  def createEngineOperationLogRootDirectory(sessionHandle: SessionHandle): Unit = {
    createOperationLogRootDirectory(ENGINE_LOG_ROOT, sessionHandle)
  }

  /**
   * The operation log root directory, this directory will delete when JVM exit.
   */
  private def createOperationLogRootDirectory(
      rootDir: String, sessionHandle: SessionHandle): Unit = {
    val path = Paths.get(rootDir, sessionHandle.identifier.toString)
    try {
      Files.createDirectories(path)
      path.toFile.deleteOnExit()
    } catch {
      case e: IOException =>
        error(s"Failed to create operation log root directory: $path", e)
    }
  }

  /**
   * Create the OperationLog for each operation with specified root dir.
   */
  private def createOperationLog(
      rootDir: String, sessionHandle: SessionHandle, opHandle: OperationHandle): OperationLog = {
    try {
      val logPath = Paths.get(rootDir, sessionHandle.identifier.toString)
      val logFile = Paths.get(logPath.toAbsolutePath.toString, opHandle.identifier.toString)
      info(s"Creating operation log file $logFile")
      new OperationLog(logFile)
    } catch {
      case e: IOException =>
        error(s"Failed to create operation log for $opHandle in $sessionHandle", e)
        null
    }
  }

  def createServerOperationLog(
      sessionHandle: SessionHandle, opHandle: OperationHandle): OperationLog = {
    createOperationLog(SERVER_LOG_ROOT, sessionHandle, opHandle)
  }

  def createEngineOperationLog(
      sessionHandle: SessionHandle, opHandle: OperationHandle): OperationLog = {
    createOperationLog(ENGINE_LOG_ROOT, sessionHandle, opHandle)
  }
}

class OperationLog(path: Path) {

  private val writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
  private val reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)

  /**
   * write log to the operation log file
   */
  def write(msg: String): Unit = synchronized {
    try {
      writer.write(msg)
      writer.flush()
    } catch {
      case _: IOException => // TODO: better do nothing?
    }
  }

  /**
   * Read to log file line by line
   *
   * @param maxRows maximum result number can reach
   */
  def read(maxRows: Int): TRowSet = synchronized {
    val logs = new java.util.ArrayList[String]
    var i = 0
    try {
      var line: String = reader.readLine()
      while ((i < maxRows || maxRows <= 0) && line != null) {
        logs.add(line)
        line = reader.readLine()
        i += 1
      }
    } catch {
      case e: IOException =>
        val absPath = path.toAbsolutePath
        val opHandle = absPath.getFileName
        throw KyuubiSQLException(s"Operation[$opHandle] log file $absPath is not found", e)
    }
    val tColumn = TColumn.stringVal(new TStringColumn(logs, ByteBuffer.allocate(0)))
    val tRow = new TRowSet(0, new java.util.ArrayList[TRow](logs.size()))
    tRow.addToColumns(tColumn)
    tRow
  }

  def close(): Unit = synchronized {
    try {
      reader.close()
      writer.close()
      Files.delete(path)
    } catch {
      case e: IOException =>
        // Printing log here may cause a deadlock. The lock order of OperationLog.write
        // is RootLogger -> LogDivertAppender -> OperationLog. If printing log here, the
        // lock order is OperationLog -> RootLogger. So the exception is thrown and
        // processing at the invocations
        throw new IOException(
          s"Failed to remove corresponding log file of operation: ${path.toAbsolutePath}", e)
    }
  }
}
