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

import java.nio.file.{Files, Path, Paths}
import java.time.Duration

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api.ContentResponse

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf

class MetricsSystemSuite extends KyuubiFunSuite {

  def checkJsonFileMetrics(path: Path, searchKey: String): Unit = {
    eventually(timeout(10.seconds), interval(500.milliseconds)) {
      val reader = Files.newBufferedReader(path)
      val logs = new java.util.ArrayList[String]
      var line = reader.readLine()
      while (line != null) {
        logs.add(line)
        line = reader.readLine()
      }
      assert(logs.asScala.exists(_.contains(searchKey)))
    }
  }

  test("metrics - PrometheusReporter") {
    val testContextPath = "/prometheus-metrics"

    val conf = KyuubiConf()
      .set(MetricsConf.METRICS_ENABLED, true)
      .set(MetricsConf.METRICS_REPORTERS, Seq(ReporterType.PROMETHEUS.toString))
      .set(MetricsConf.METRICS_PROMETHEUS_PORT, 0) // random port
      .set(MetricsConf.METRICS_PROMETHEUS_PATH, testContextPath)
    val metricsSystem = new MetricsSystem()
    metricsSystem.initialize(conf)
    metricsSystem.start()

    metricsSystem.registerGauge(MetricsConstants.CONN_OPEN, 2021, 0)

    val prometheusHttpServer = metricsSystem.getServices.head
      .asInstanceOf[PrometheusReporterService].httpServer

    val client: HttpClient = new HttpClient
    client.start()
    val res: ContentResponse = client.GET(prometheusHttpServer.getURI.resolve(testContextPath))
    assert(res.getContentAsString.contains("heap_usage"))
    assert(res.getContentAsString.contains("kyuubi_connection_opened 2021.0"))
    client.stop()

    metricsSystem.stop()
  }

  test("metrics - other reporters") {
    val reportPath = Utils.createTempDir()
    val conf = KyuubiConf()
      .set(MetricsConf.METRICS_ENABLED, true)
      .set(MetricsConf.METRICS_REPORTERS,
        ReporterType.values.filterNot(_ == ReporterType.PROMETHEUS).map(_.toString).toSeq)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofSeconds(1).toMillis)
      .set(MetricsConf.METRICS_JSON_LOCATION, reportPath.toString)
    val metricsSystem = new MetricsSystem()
    metricsSystem.initialize(conf)
    metricsSystem.start()
    val reportFile = Paths.get(reportPath.toString, "report.json")
    checkJsonFileMetrics(reportFile, "heap.usage")
    metricsSystem.incCount(MetricsConstants.STATEMENT_TOTAL)

    checkJsonFileMetrics(reportFile, MetricsConstants.STATEMENT_TOTAL)
    metricsSystem.decCount(MetricsConstants.STATEMENT_TOTAL)
    metricsSystem.registerGauge(MetricsConstants.CONN_OPEN, 20181117, 0)
    checkJsonFileMetrics(reportFile, MetricsConstants.CONN_OPEN)
    checkJsonFileMetrics(reportFile, "20181117")
    metricsSystem.stop()
  }
}
