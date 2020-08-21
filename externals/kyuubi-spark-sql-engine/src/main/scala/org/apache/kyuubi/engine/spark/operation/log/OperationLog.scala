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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils
import org.apache.hive.service.rpc.thrift.{TColumn, TRow, TRowSet, TStringColumn}

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.session.SessionHandle

/**
 * TODO: This part is spark-independent, mv to kyuubi-common?
 * we can do this after we decide to support other engines
 */
object OperationLog extends Logging {

  final val LOG_ROOT: String = "kyuubi_operation_logs"
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

  /**
   * The operation log root directory, here we choose $PWD/[[LOG_ROOT]]/$sessionId/ as the root per
   * session, this directory will delete when JVM exit.
   */
  def createOperationLogRootDirectory(sessionHandle: SessionHandle): Unit = {
    val path = Paths.get(LOG_ROOT, sessionHandle.identifier.toString)
    try {
      Files.createDirectories(path)
      path.toFile.deleteOnExit()
    } catch {
      case NonFatal(e) =>
        error(s"Failed to create operation log root directory: $path", e)
    }
  }

  /**
   * Create the OperationLog for each operation, the log file will be located at
   * $PWD/[[LOG_ROOT]]/$sessionId/$operationId
   * @return
   */
  def createOperationLog(sessionHandle: SessionHandle, opHandle: OperationHandle): OperationLog = {
    try {
      val logPath = Paths.get(LOG_ROOT, sessionHandle.identifier.toString)
      val logFile = Paths.get(logPath.toAbsolutePath.toString, opHandle.identifier.toString)
      Files.createFile(logFile)
      info(s"Created operation log file $logFile")
      new OperationLog(logFile)
    } catch {
      case e: IOException =>
        error(s"Failed to create operation log for $opHandle in $sessionHandle", e)
        null
    }
  }
}

class OperationLog(path: Path) extends Logging {

  private lazy val writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
  private lazy val reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)

  /**
   * write log to the operation log file
   */
  def write(msg: String): Unit = synchronized {
    writer.write(msg)
    writer.flush()
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
        throw new KyuubiSQLException(s"Operation[$opHandle] log file $absPath is not found", e)
    }
    val tColumn = TColumn.stringVal(new TStringColumn(logs, ByteBuffer.allocate(0)))
    val tRow = new TRowSet(0, new java.util.ArrayList[TRow](0))
    tRow.addToColumns(tColumn)
    tRow
  }

  def close(): Unit = synchronized {
    try {
      reader.close()
      writer.close()
      FileUtils.forceDelete(path.toFile)
    } catch {
      case e: IOException =>
        error(s"Failed to remove corresponding log file of operation: ${path.toAbsolutePath}", e)
    }
  }
}
