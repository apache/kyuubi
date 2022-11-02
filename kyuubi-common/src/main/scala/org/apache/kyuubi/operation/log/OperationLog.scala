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

import java.io.{BufferedReader, IOException}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.hive.service.rpc.thrift.{TColumn, TRow, TRowSet, TStringColumn}

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThriftUtils

object OperationLog extends Logging {
  final private val OPERATION_LOG: InheritableThreadLocal[OperationLog] = {
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
   * The operation log root directory, this directory will delete when JVM exit.
   */
  def createOperationLogRootDirectory(session: Session): Unit = {
    session.sessionManager.operationLogRoot.foreach { operationLogRoot =>
      val path = Paths.get(operationLogRoot, session.handle.identifier.toString)
      try {
        Files.createDirectories(path)
        path.toFile.deleteOnExit()
      } catch {
        case e: IOException =>
          error(s"Failed to create operation log root directory: $path", e)
      }
    }
  }

  /**
   * Create the OperationLog for each operation.
   */
  def createOperationLog(session: Session, opHandle: OperationHandle): OperationLog = {
    session.sessionManager.operationLogRoot.map { operationLogRoot =>
      try {
        val logPath = Paths.get(operationLogRoot, session.handle.identifier.toString)
        val logFile = Paths.get(logPath.toAbsolutePath.toString, opHandle.identifier.toString)
        info(s"Creating operation log file $logFile")
        new OperationLog(logFile)
      } catch {
        case e: IOException =>
          error(s"Failed to create operation log for $opHandle in ${session.handle}", e)
          null
      }
    }.orNull
  }
}

class OperationLog(path: Path) {

  private lazy val writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
  private lazy val reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)

  @volatile private var initialized: Boolean = false

  private lazy val extraPaths: ListBuffer[Path] = ListBuffer()
  private lazy val extraReaders: ListBuffer[BufferedReader] = ListBuffer()
  private var lastSeekReadPos = 0
  private var seekableReader: SeekableBufferedReader = _

  def addExtraLog(path: Path): Unit = synchronized {
    try {
      extraReaders += Files.newBufferedReader(path, StandardCharsets.UTF_8)
      extraPaths += path
      Option(seekableReader).foreach(_.close)
      seekableReader = null
    } catch {
      case _: IOException =>
    }
  }

  /**
   * write log to the operation log file
   */
  def write(msg: String): Unit = synchronized {
    try {
      writer.write(msg)
      writer.flush()
      initOperationLogIfNecessary()
    } catch {
      case _: IOException => // TODO: better do nothing?
    }
  }

  private[log] def initOperationLogIfNecessary(): Unit = {
    if (!initialized) initialized = true
  }

  private def readLogs(
      reader: BufferedReader,
      lastRows: Int,
      maxRows: Int): (JArrayList[String], Int) = {
    val logs = new JArrayList[String]
    var i = 0
    try {
      var line: String = reader.readLine()
      while ((i < lastRows || maxRows <= 0) && line != null) {
        logs.add(line)
        line = reader.readLine()
        i += 1
      }
      (logs, i)
    } catch {
      case e: IOException =>
        val absPath = path.toAbsolutePath
        val opHandle = absPath.getFileName
        throw KyuubiSQLException(s"Operation[$opHandle] log file $absPath is not found", e)
    }
  }

  private def toRowSet(logs: JList[String]): TRowSet = {
    val tColumn = TColumn.stringVal(new TStringColumn(logs, ByteBuffer.allocate(0)))
    val tRow = new TRowSet(0, new JArrayList[TRow](logs.size()))
    tRow.addToColumns(tColumn)
    tRow
  }

  /**
   * Read to log file line by line
   *
   * @param maxRows maximum result number can reach
   */
  def read(maxRows: Int): TRowSet = synchronized {
    if (!initialized) return ThriftUtils.newEmptyRowSet
    val (logs, lines) = readLogs(reader, maxRows, maxRows)
    var lastRows = maxRows - lines
    for (extraReader <- extraReaders if lastRows > 0 || maxRows <= 0) {
      val (extraLogs, extraRows) = readLogs(extraReader, lastRows, maxRows)
      lastRows = lastRows - extraRows
      logs.addAll(extraLogs)
    }

    toRowSet(logs)
  }

  def read(from: Int, size: Int): TRowSet = synchronized {
    if (!initialized) return ThriftUtils.newEmptyRowSet
    var pos = from
    if (pos < 0) {
      // just fetch forward
      pos = lastSeekReadPos
    }
    if (seekableReader == null) {
      seekableReader = new SeekableBufferedReader(Seq(path) ++ extraPaths)
    } else {
      // if from < last pos, we should reload the reader
      // otherwise, we can reuse the existed reader for better performance
      if (pos < lastSeekReadPos) {
        seekableReader.close()
        seekableReader = new SeekableBufferedReader(Seq(path) ++ extraPaths)
      }
    }

    val it = seekableReader.readLine(pos, size)
    val res = it.toList.asJava
    lastSeekReadPos = pos + res.size()
    toRowSet(res)
  }

  def close(): Unit = synchronized {
    closeExtraReaders()

    trySafely {
      reader.close()
    }
    trySafely {
      writer.close()
    }

    if (seekableReader != null) {
      lastSeekReadPos = 0
      trySafely {
        seekableReader.close()
      }
    }

    trySafely {
      Files.delete(path)
    }
  }

  private def trySafely(f: => Unit): Unit = {
    try {
      f
    } catch {
      case e: IOException =>
        // Printing log here may cause a deadlock. The lock order of OperationLog.write
        // is RootLogger -> LogDivertAppender -> OperationLog. If printing log here, the
        // lock order is OperationLog -> RootLogger. So the exception is thrown and
        // processing at the invocations
        throw new IOException(
          s"Failed to remove corresponding log file of operation: ${path.toAbsolutePath}",
          e)
    }
  }

  private def closeExtraReaders(): Unit = {
    extraReaders.foreach { extraReader =>
      try {
        extraReader.close()
      } catch {
        case _: IOException => // for the outside log file reader, ignore it
      }
    }
  }
}
