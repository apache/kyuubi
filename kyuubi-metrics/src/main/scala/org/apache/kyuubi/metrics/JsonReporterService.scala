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

import java.io.BufferedWriter
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
  private var reportDir: Path = _
  private var reportPath: Path = _

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    reportDir = Paths.get(conf.get(METRICS_JSON_LOCATION)).toAbsolutePath
    Files.createDirectories(reportDir)
    reportPath = Paths.get(reportDir.toString, "report.json").toAbsolutePath
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    val interval = conf.get(METRICS_JSON_INTERVAL)
    var writer: BufferedWriter = null
    timer.schedule(new TimerTask {
      override def run(): Unit = try {
        val json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(registry)
        val tmpPath = Files.createTempFile(reportDir, "report", ".json").toAbsolutePath
        writer = Files.newBufferedWriter(tmpPath, StandardCharsets.UTF_8)
        writer.write(json)
        writer.close()
        Files.setPosixFilePermissions(tmpPath, PosixFilePermissions.fromString("rwxr--r--"))
        Files.move(tmpPath, reportPath, StandardCopyOption.REPLACE_EXISTING)
      } catch {
        case NonFatal(e) => error("Error writing metrics to json file" + reportPath, e)
      } finally {
        if (writer != null) writer.close()
      }
    }, interval, interval)
    super.start()
  }

  override def stop(): Unit = synchronized {
    timer.cancel()
    super.stop()
  }
}
