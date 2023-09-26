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
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.{MetricsConf, ReporterType}
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class BackendServiceMetricSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  val reportPath: Path = Utils.createTempDir()
  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(MetricsConf.METRICS_REPORTERS, Set(ReporterType.JSON.toString))
      .set(MetricsConf.METRICS_JSON_LOCATION, reportPath.toString)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofMillis(100).toMillis)
  }

  test("backend service metric test") {
    val objMapper = new ObjectMapper()

    withJdbcStatement() { statement =>
      statement.executeQuery("CREATE TABLE stu_test(id int, name string) USING parquet")
      statement.execute("insert into stu_test values(1, 'a'), (2, 'b'), (3, 'c')")
      val logRows1 = eventually(timeout(10.seconds), interval(1.second)) {
        val res = objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)
        assert(res.has("timers"))
        val timer = res.get("timers")
        assert(timer.get(BS_EXECUTE_STATEMENT).get("count").asInt() == 2)
        assert(timer.get(BS_EXECUTE_STATEMENT).get("mean").asDouble() > 0)

        assert(res.has("meters"))
        val meters = res.get("meters")
        val logRows = meters.get(BS_FETCH_LOG_ROWS_RATE).get("count").asInt()
        assert(logRows > 0)
        logRows
      }

      statement.execute("select * from stu_test limit 2")
      statement.getResultSet.next()
      eventually(timeout(60.seconds), interval(1.second)) {
        val res = objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)
        val timer = res.get("timers")
        assert(timer.get(BS_OPEN_SESSION).get("count").asInt() == 1)
        assert(timer.get(BS_OPEN_SESSION).get("min").asDouble() > 0)
        val execStatementNode = timer.get(BS_EXECUTE_STATEMENT)
        assert(execStatementNode.get("count").asInt() == 3)
        assert(
          execStatementNode.get("max").asDouble() >= execStatementNode.get("mean").asDouble() &&
            execStatementNode.get("mean").asDouble() >= execStatementNode.get("min").asDouble())

        val meters =
          objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile).get("meters")
        assert(meters.get(BS_FETCH_RESULT_ROWS_RATE).get("count").asInt() == 8)
        assert(meters.get(BS_FETCH_LOG_ROWS_RATE).get("count").asInt() >= logRows1)

        statement.executeQuery("DROP TABLE stu_test")
      }
    }
  }
}
