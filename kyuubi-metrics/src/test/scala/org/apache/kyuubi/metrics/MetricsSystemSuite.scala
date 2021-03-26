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

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf

class MetricsSystemSuite extends KyuubiFunSuite {

  def checkMetrics(path: Path, searchKey: String): Unit = {
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

  test("MetricsSystem") {
    val reportPath = Utils.createTempDir()
    val conf = KyuubiConf()
      .set(MetricsConf.METRICS_ENABLED, true)
      .set(MetricsConf.METRICS_REPORTERS, ReporterType.values.map(_.toString).toSeq)
      .set(MetricsConf.METRICS_REPORT_INTERVAL, Duration.ofSeconds(1).toMillis)
      .set(MetricsConf.METRICS_REPORT_LOCATION, reportPath.toString)
    val metricsSystem = new MetricsSystem()
    metricsSystem.initialize(conf)
    metricsSystem.start()
    val reportFile = Paths.get(reportPath.toString, "report.json")
    checkMetrics(reportFile, "PS-MarkSweep.count")
    metricsSystem.incAndGetCount(MetricsConstants.STATEMENT_TOTAL)

    checkMetrics(reportFile, MetricsConstants.STATEMENT_TOTAL)
    metricsSystem.decAndGetCount(MetricsConstants.STATEMENT_TOTAL)
    metricsSystem.registerGauge(MetricsConstants.CONN_OPEN, 20181117, 0)
    checkMetrics(reportFile, MetricsConstants.CONN_OPEN)
    checkMetrics(reportFile, "20181117")
    metricsSystem.stop()
  }

}
