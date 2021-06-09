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

package org.apache.kyuubi.engine

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.StringUtils.containsIgnoreCase

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.EngineProcess.{PROC_BUILD_LOGGER, UNCAUGHT_ERROR}
import org.apache.kyuubi.util.NamedThreadFactory

/**
 * A wrapper of Process
 */
trait IEngineProcess {
  def start(): Unit
  def stop(): Unit

  def checkExited(): Boolean
  def isAlive(): Boolean
  def getExitValue(): Int
  def getError(): Throwable
}
object EngineProcess {
  val UNCAUGHT_ERROR = new RuntimeException("Uncaught error")
  val PROC_BUILD_LOGGER = new NamedThreadFactory("process-logger-capture", daemon = true)
}

class EngineProcess(processBuilder: ProcessBuilder, conf: KyuubiConf, engineLog: File)
  extends IEngineProcess {
  assert(processBuilder != null)

  private var process: Process = _
  // Visible for test
  private[kyuubi] var logCaptureThread: Thread = _
  @volatile private var error: Throwable = UNCAUGHT_ERROR
  @volatile private var lastRowOfLog: String = "unknown"

  override def start(): Unit = {
    assert(process == null)
    process = processBuilder.start()
    val reader = Files.newBufferedReader(engineLog.toPath, StandardCharsets.UTF_8)

    val redirect: Runnable = { () =>
      try {
        val maxErrorSize = conf.get(KyuubiConf.ENGINE_ERROR_MAX_SIZE)
        var line: String = reader.readLine
        while (true) {
          if (containsIgnoreCase(line, "Exception:") &&
            !line.contains("at ") && !line.startsWith("Caused by:")) {
            val sb = new StringBuilder(line)
            error = KyuubiSQLException(sb.toString() + s"\n See more: $engineLog")
            line = reader.readLine()
            while (sb.length < maxErrorSize && line != null &&
              (line.startsWith("\tat ") || line.startsWith("Caused by: "))) {
              sb.append("\n" + line)
              line = reader.readLine()
            }

            error = KyuubiSQLException(sb.toString() + s"\n See more: $engineLog")
          } else if (line != null) {
            lastRowOfLog = line
          }
          line = reader.readLine()
        }
      } catch {
        case _: IOException =>
        case _: InterruptedException =>
      } finally {
        reader.close()
      }
    }

    logCaptureThread = PROC_BUILD_LOGGER.newThread(redirect)
    logCaptureThread.start()
  }

  override def checkExited(): Boolean = {
    process.waitFor(1L, TimeUnit.SECONDS)
  }

  override def getExitValue(): Int = {
    process.exitValue()
  }

  override def isAlive(): Boolean = {
    process.isAlive
  }

  override def getError(): Throwable = {
    if (error == UNCAUGHT_ERROR) {
      Thread.sleep(1000)
    }
    error match {
      case UNCAUGHT_ERROR =>
        KyuubiSQLException(s"Failed to detect the root cause, please check $engineLog at server " +
          s"side if necessary. The last line log is: $lastRowOfLog")
      case other => other
    }
  }

  override def stop(): Unit = {
    if (process != null) {
      process.destroyForcibly()
    }
    if (logCaptureThread != null) {
      logCaptureThread.interrupt()
    }
  }
}
