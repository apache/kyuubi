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

package org.apache.kyuubi.metrics

import java.io.{BufferedWriter, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.PosixFilePermissions
import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.MetricsConf._
import org.apache.kyuubi.service.AbstractService

class JsonReporterService(registry: MetricRegistry)
  extends AbstractService("JsonReporterService") {
  private val jsonMapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, false))
  private val timer = new Timer(true)
  private var tempPath: Path = _
  private var reportPath: Path = _
  private var writer: BufferedWriter = _

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val reportDir = conf.get(METRICS_REPORT_LOCATION)
    reportPath = Paths.get(reportDir, "report.json").toAbsolutePath
    tempPath = Paths.get(reportDir, "report.json.tmp").toAbsolutePath
    writer = Files.newBufferedWriter(
      tempPath, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    val interval = conf.get(METRICS_REPORT_INTERVAL)
    timer.schedule(new TimerTask {
      override def run(): Unit = try {
        val json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(registry)
        writer.write(json)
        writer.flush()
        Files.setPosixFilePermissions(tempPath, PosixFilePermissions.fromString("rwxr--r--"))
        Files.move(tempPath, reportPath, StandardCopyOption.REPLACE_EXISTING)
      } catch {
        case NonFatal(e) =>
          error("Error writing metrics to json file" + tempPath, e)
      }
    }, interval, interval)
    super.start()
  }

  override def stop(): Unit = synchronized {
    try {
      if (writer != null) writer.close()
      timer.cancel()
    } catch {
      case _: IOException =>
    }
    super.stop()
  }
}
