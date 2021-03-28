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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Duration

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.log4j.Level

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.micrometer.MicrometerMetricsService

class MetricsSuite extends KyuubiFunSuite {

  var ms: MicrometerMetricsService = _
  var jsonReportPath: Path = _

  override def beforeAll(): Unit = {
    jsonReportPath = Utils.createTempDir()
    val conf = KyuubiConf()
      .set(MetricsConf.METRICS_ENABLED, true)
      .set(MetricsConf.METRICS_HISTOGRAM, true)
      .set(MetricsConf.METRICS_EXPORTERS, ExporterType.values.map(_.toString).toSeq)
      .set(MetricsConf.METRICS_JSON_LOCATION, jsonReportPath.toString)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofSeconds(1).toMillis)
      .set(MetricsConf.METRICS_SLF4J_LOGGER, "kyuubi.metrics")
      .set(MetricsConf.METRICS_SLF4J_INTERVAL, Duration.ofSeconds(1).toMillis)
      .set(MetricsConf.METRICS_SLF4J_LOG_INACTIVE, true)
      .set(MetricsConf.METRICS_PROMETHEUS_HOST, "localhost")
      .set(MetricsConf.METRICS_PROMETHEUS_PORT, 10090)
      .set(MetricsConf.METRICS_PROMETHEUS_CONTEXT_PATH, "/metrics")
      .set(MetricsConf.METRICS_PROMETHEUS_INTERVAL, Duration.ofSeconds(1).toMillis)
      .set(MetricsConf.METRICS_PROMETHEUS_DESCRIPTION, false)
    ms = new MicrometerMetricsService()
    ms.initialize(conf)
    ms.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    ms.stop()
    // TODO delete jsonReportPath
    super.afterAll()
  }

  test("SLF4J metrics exporter") {

    def check(logAppender: LogAppender, searchKey: String): Unit =
      eventually(timeout(3.seconds), interval(500.milliseconds)) {
        assert(logAppender.loggingEvents.exists(_.getRenderedMessage.contains(searchKey)))
      }

    val logAppender = new LogAppender()
    withLogAppender(logAppender, Some("kyuubi.metrics"), Some(Level.INFO)) {
      check(logAppender, "jvm.threads.states{state=runnable}")

      Metrics.count(OPERATION, T_EVT, EVT_OPEN)(1)
      check(logAppender, s"$OPERATION{$T_EVT=$EVT_OPEN}")

      Metrics.gauge(SERVICE_EXEC_POOL, T_STAT, STAT_ACTIVE)(20181117)
      check(logAppender, s"$SERVICE_EXEC_POOL{$T_STAT=$STAT_ACTIVE}")
      assert(ms.registry.find(SERVICE_EXEC_POOL).tag(T_STAT, STAT_ACTIVE).gauge().value() == 20181117)
    }
  }

  test("JSON metrics exporter") {
    print(jsonReportPath.toAbsolutePath)

    def check(path: Path, searchKey: String): Unit =
      eventually(timeout(3.seconds), interval(500.milliseconds)) {
        val logs = Files.readAllLines(path, StandardCharsets.UTF_8)
        assert(logs.asScala.exists(_.contains(searchKey)))
      }

    Metrics.gauge(SERVICE_EXEC_POOL, T_STAT, STAT_ACTIVE)(20181117)
    check(Paths.get(jsonReportPath.toAbsolutePath.toString, "report.json"), "20181117")
  }
}
