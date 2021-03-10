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
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils.containsIgnoreCase

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.util.NamedThreadFactory

trait ProcBuilder {
  import ProcBuilder._

  protected def executable: String

  protected def mainResource: Option[String]

  protected def module: String

  protected def mainClass: String

  protected def proxyUser: String

  protected def commands: Array[String]

  protected def conf: KyuubiConf

  protected def env: Map[String, String]

  protected val workingDir: Path

  final lazy val processBuilder: ProcessBuilder = {
    val pb = new ProcessBuilder(commands: _*)

    val envs = pb.environment()
    envs.putAll(env.asJava)
    pb.directory(workingDir.toFile)
    pb.redirectError(engineLog)
    pb.redirectOutput(engineLog)
    pb
  }

  @volatile private var error: Throwable = UNCAUGHT_ERROR
  // Visible for test
  private[kyuubi] var logCaptureThread: Thread = _

  private[kyuubi] lazy val engineLog: File = ProcBuilder.synchronized {
    val engineLogTimeout = conf.get(KyuubiConf.ENGINE_LOG_TIMEOUT)
    val currentTime = System.currentTimeMillis()
    val processLogPath = workingDir
    val totalExistsFile = processLogPath.toFile.listFiles { (_, name) => name.startsWith(module) }
    val sorted = totalExistsFile.sortBy(_.getName.split("\\.").last.toInt)
    val nextIndex = if (sorted.isEmpty) {
      0
    } else {
      sorted.last.getName.split("\\.").last.toInt + 1
    }
    val file = sorted.find(_.lastModified() < currentTime - engineLogTimeout)
      .map { existsFile =>
        try {
          // Here we want to overwrite the exists log file
          existsFile.delete()
          existsFile.createNewFile()
          existsFile
        } catch {
          case e: Exception =>
            warn(s"failed to delete engine log file: ${existsFile.getAbsolutePath}", e)
            null
        }
      }
      .getOrElse {
        Files.createDirectories(processLogPath)
        val newLogFile = new File(processLogPath.toFile, s"$module.log.$nextIndex")
        newLogFile.createNewFile()
        newLogFile
      }
    file.setLastModified(currentTime)
    info(s"Logging to $file")
    file
  }

  final def start: Process = synchronized {

    val proc = processBuilder.start()
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
    proc
  }

  def close(): Unit = {
    if (logCaptureThread != null) {
      logCaptureThread.interrupt()
    }
  }

  def getError: Throwable = synchronized {
    if (error == UNCAUGHT_ERROR) {
      Thread.sleep(1000)
    }
    error match {
      case UNCAUGHT_ERROR => KyuubiSQLException(
        s"Failed to detect the root cause, please check $engineLog at server side if necessary")
      case other => other
    }
  }
}

object ProcBuilder extends Logging {
  private val PROC_BUILD_LOGGER = new NamedThreadFactory("process-logger-capture", daemon = true)

  private val UNCAUGHT_ERROR = new RuntimeException("Uncaught error")

}
