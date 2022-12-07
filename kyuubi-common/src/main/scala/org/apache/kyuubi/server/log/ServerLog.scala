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

package org.apache.kyuubi.server.log

import java.io.{BufferedReader, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.{ArrayList => JArrayList}

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SERVER_EVENT_JSON_LOG_PATH
import org.apache.kyuubi.operation.log.SeekableBufferedReader

object ServerLog extends Logging {
  final private val SERVER_LOG: InheritableThreadLocal[ServerLog] = {
    new InheritableThreadLocal[ServerLog] {
      override def initialValue(): ServerLog = null
    }
  }

  def setCurrentServerLog(serverLog: ServerLog): Unit = {
    SERVER_LOG.set(serverLog)
  }

  def getCurrentServerLog: ServerLog = SERVER_LOG.get()

  def removeCurrentServerLog(): Unit = SERVER_LOG.remove()

  def createServerLog(conf: KyuubiConf): ServerLog = {
    try {
      val logPath = conf.get(SERVER_EVENT_JSON_LOG_PATH)
      val logFile = Paths.get(logPath)
      info(s"Creating server log $logFile")
      new ServerLog(logFile)
    } catch {
      case e: IOException =>
        error(s"Failed to create server log", e)
        null
    }
  }

}

class ServerLog(path: Path) {

  private lazy val writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
  private lazy val reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)

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
        throw new KyuubiException(s"Operation[$opHandle] log file $absPath is not found", e)
    }
  }

  /**
   * Read to log file line by line
   *
   * @param maxRows maximum result number can reach
   */
  def read(maxRows: Int): JArrayList[String] = synchronized {
    val (logs, lines) = readLogs(reader, maxRows, maxRows)
    var lastRows = maxRows - lines
    for (extraReader <- extraReaders if lastRows > 0 || maxRows <= 0) {
      val (extraLogs, extraRows) = readLogs(extraReader, lastRows, maxRows)
      lastRows = lastRows - extraRows
      logs.addAll(extraLogs)
    }
    logs
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
