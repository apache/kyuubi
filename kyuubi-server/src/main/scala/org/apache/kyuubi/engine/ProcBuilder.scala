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
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.util.matching.Regex

import org.apache.commons.lang3.StringUtils.containsIgnoreCase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.net.minidev.json.JSONObject
import org.apache.hadoop.shaded.org.apache.http.client.methods.{CloseableHttpResponse, HttpPut}
import org.apache.hadoop.shaded.org.apache.http.entity.{ContentType, StringEntity}
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients
import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils

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

  protected def env: Map[String, String] = conf.getEnvs

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
  @volatile private var lastRowOfLog: String = "unknown"
  // Visible for test
  @volatile private[kyuubi] var logCaptureThreadReleased: Boolean = true
  private var logCaptureThread: Thread = _

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
        while (true) {
          if (reader.ready()) {
            var line: String = reader.readLine
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
          } else {
            Thread.sleep(300)
          }
        }
      } catch {
        case _: IOException =>
        case _: InterruptedException =>
      } finally {
        logCaptureThreadReleased = true
        reader.close()
      }
    }

    logCaptureThreadReleased = false
    logCaptureThread = PROC_BUILD_LOGGER.newThread(redirect)
    logCaptureThread.start()
    proc
  }

  def getAddr: List[String] = {
    val conf: Configuration = new Configuration(false)
    val yarnConf = System.getenv("HADOOP_CONF_DIR") match {
      case null => throw KyuubiSQLException(
        s"Failed to kill yarn job. HADOOP_CONF_DIR is not set! " +
          "For more detail information on installing and configuring Spark, please visit " +
          "https://kyuubi.apache.org/docs/stable/deployment/settings.html#environments")
      case value => value + "yarn-site.xml"
    }
    conf.addResource(new org.apache.hadoop.fs.Path(yarnConf))

    var hostAndIp: Map[String, String] = Map()
    conf foreach {
      case property if property.getKey.contains(YARN_ADDRESS) =>
        val s = property.getValue.split(":")
        hostAndIp += (s(0) -> s(1))
      case property if (property.getKey.contains(YARN_HOSTNAME)
        && !hostAndIp.contains(property.getValue)) =>
        hostAndIp += (property.getValue -> "8088")
      case _ =>
    }
    hostAndIp.map(addr => s"${addr._1}:${addr._2}").toList
  }

  def execKill(url: String): Int = {
    val httpPut = new HttpPut(url)
    val params = new JSONObject
    params.put("state", "KILLED")
    val stringEntity = new StringEntity(params.toString(), ContentType.APPLICATION_JSON)
    httpPut.setEntity(stringEntity)
    val httpClient = HttpClients.createDefault()
    try {
      val response: CloseableHttpResponse = httpClient.execute(httpPut)
      val resEntity = response.getEntity
      EntityUtils.consume(resEntity)
      response.getStatusLine.getStatusCode
    } catch {
      case e: Exception =>
        info(s"Failed to request $url, due to ${e.getMessage}")
        404
    }
  }

  val YARN_APP_NAME_REGEX: Regex = "application_\\d+_\\d+".r

  def killApplication(line: String = lastRowOfLog): Unit =
    YARN_APP_NAME_REGEX.findFirstIn(line) match {
      case Some(appId) if line.contains("state: ACCEPTED") =>
        val addresses = getAddr
        var isSuccess = false
        var n = 0
        while (!isSuccess && addresses.size > n) {
          val result = execKill(s"http://${addresses(n)}/ws/v1/cluster/apps/$appId/state")
          result match {
            case _ if result == 202 || result == 200 =>
              isSuccess = true
            case _ if n == (addresses.size - 1) =>
              throw KyuubiSQLException(s"Failed to kill $appId, please kill it manually.")
            case _ =>
              n += 1
          }
        }
      case None =>
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
      case UNCAUGHT_ERROR =>
        KyuubiSQLException(s"Failed to detect the root cause, please check $engineLog at server " +
          s"side if necessary. The last line log is: $lastRowOfLog")
      case other => other
    }
  }
}

object ProcBuilder extends Logging {
  private val PROC_BUILD_LOGGER = new NamedThreadFactory("process-logger-capture", daemon = true)

  private val UNCAUGHT_ERROR = new RuntimeException("Uncaught error")

  private final val YARN_ADDRESS = "yarn.resourcemanager.webapp.address"
  private final val YARN_HOSTNAME = "yarn.resourcemanager.hostname"
}
