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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.util.NamedThreadFactory

trait ProcBuilder {
  import ProcBuilder._

  protected def executable: String

  protected def mainResource: Option[String]

  protected def module: String

  protected def mainClass: String

  protected def proxyUser: String

  protected def commands: Array[String]

  protected def env: Map[String, String]

  protected val workingDir: Path

  final lazy val processBuilder: ProcessBuilder = {
    val pb = new ProcessBuilder(commands: _*)

    val envs = pb.environment()
    envs.putAll(env.asJava)
    pb.directory(workingDir.toFile)
    pb
  }

  @volatile private var error: Throwable = UNCAUGHT_ERROR

  final def start: Process = synchronized {
    val procLog = Paths.get(workingDir.toAbsolutePath.toString, s"$module.log")
    processBuilder.redirectError(procLog.toFile)
    processBuilder.redirectOutput(procLog.toFile)

    val proc = processBuilder.start()
    val reader = Files.newBufferedReader(procLog, StandardCharsets.UTF_8)

    val redirect = new Runnable {
      override def run(): Unit = try {
        var line: String = reader.readLine
        while (true) {
          if (containsIgnoreCase(line, "Exception") && !line.contains("at ") &&
            !line.startsWith("Caused by:")) {
            val sb = new StringBuilder(line)

            line = reader.readLine()
            while (line != null && (line.startsWith("\tat ") || line.startsWith("Caused by: "))) {
              sb.append("\n" + line)
              line = reader.readLine()
            }

            error = KyuubiSQLException(sb.toString())
          }
          line = reader.readLine()
        }
      } catch {
        case _: IOException =>
      } finally {
        reader.close()
      }
    }

    PROC_BUILD_LOGGER.newThread(redirect).start()
    proc
  }

  def getError: Throwable = synchronized {
    if (error == UNCAUGHT_ERROR) {
      Thread.sleep(3000)
    }
    error
  }
}

object ProcBuilder {
  private val PROC_BUILD_LOGGER = new NamedThreadFactory("process-logger-capture", daemon = true)

  private val UNCAUGHT_ERROR = KyuubiSQLException("Uncaught error")

  def containsIgnoreCase(str: String, searchStr: String): Boolean = {
    if (str == null || searchStr == null) {
      false
    } else {
      val max = str.length - searchStr.length
      var i = 0
      while (i <= max) {
        if (str.regionMatches(true, i, searchStr, 0, searchStr.length)) {
          return true
        }
        i += 1
      }
      false
    }
  }

}
