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

package org.apache.kyuubi.server

import java.nio.file.{Path, Paths}
import java.time.Duration

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.{MetricsConf, MetricsConstants}
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class BackendServiceTimeMetricSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  val reportPath: Path = Utils.createTempDir()
  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(MetricsConf.METRICS_REPORTERS, Seq("JSON"))
      .set(MetricsConf.METRICS_JSON_LOCATION, reportPath.toString)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofMillis(100).toMillis)
  }

  test("backend service method time metric test") {
    val objMapper = new ObjectMapper()

    withJdbcStatement() { statement =>
      statement.execute("show databases")
      Thread.sleep(Duration.ofMillis(111).toMillis)

      val res1 = objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)
      assert(res1.has("timers"))
      val histograms1 = res1.get("timers")
      assert(
        histograms1.get(MetricsConstants.BS_EXECUTE_STATEMENT).get("count").asInt() == 1)
      assert(
        histograms1.get(MetricsConstants.BS_EXECUTE_STATEMENT).get("mean").asDouble() > 0)

      statement.execute("show tables")
      Thread.sleep(Duration.ofMillis(111).toMillis)

      val res2 = objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)
      val histograms2 = res2.get("timers")
      assert(
        histograms2.get(MetricsConstants.BS_OPEN_SESSION).get("count").asInt() == 1)
      assert(
        histograms2.get(MetricsConstants.BS_OPEN_SESSION).get("min").asInt() > 0)
      val execStatementNode2 = histograms2.get(MetricsConstants.BS_EXECUTE_STATEMENT)
      assert(execStatementNode2.get("count").asInt() == 2)
      assert(
        execStatementNode2.get("max").asDouble() >= execStatementNode2.get("mean").asDouble() &&
          execStatementNode2.get("mean").asDouble() >= execStatementNode2.get("min").asDouble())
    }
  }
}
