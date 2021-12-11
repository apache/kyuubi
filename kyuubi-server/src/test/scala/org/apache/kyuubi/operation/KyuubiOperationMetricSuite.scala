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

package org.apache.kyuubi.operation

import java.nio.file.{Path, Paths}
import java.time.Duration

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.MetricsConf

class KyuubiOperationMetricSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  val reportPath: Path = Utils.createTempDir()

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(MetricsConf.METRICS_REPORTERS, Seq("JSON"))
      .set(MetricsConf.METRICS_JSON_LOCATION, reportPath.toString)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofMillis(100).toMillis)
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("test Kyuubi operation metric") {
    val objMapper = new ObjectMapper()
    withJdbcStatement() { statement =>
      statement.executeQuery("show databases")
      statement.executeQuery("show tables")

      Thread.sleep(Duration.ofMillis(111).toMillis)
      val res1 = objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)
      assert(res1.has("counters"))
      val counter1 = res1.get("counters")
      assert(counter1.has(s"kyuubi.connection.opened.$user"))
      assert(counter1.get(s"kyuubi.connection.opened.$user").get("count").asInt() == 1)
      assert(counter1.get("kyuubi.connection.total").get("count").asInt() == 1)
      assert(counter1.get("kyuubi.operation.total.execute_statement").get("count").asInt() == 2)
      assert(counter1.get("kyuubi.operation.opened.launch_engine").get("count").asInt() == 1)
      assert(counter1.get("kyuubi.operation.total").get("count").asInt() == 3)

      try {
        statement.executeQuery("fail execute")
      } catch {
        case _: Exception => // do not thing
      }
      statement.close()

      Thread.sleep(Duration.ofMillis(111).toMillis)
      val res2 = objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)
      val counter2 = res2.get("counters")
      assert(counter1.get("kyuubi.engine.total").get("count").asInt() == 1)
      assert(counter2.get("kyuubi.operation.total.execute_statement").get("count").asInt() == 3)
      assert(counter2.get("kyuubi.operation.opened.execute_statement").get("count").asInt() == 0)
      assert(counter2.has("kyuubi.operation.failed.execute_statement.KyuubiSQLException"))
      assert(
        counter2
          .get("kyuubi.operation.failed.execute_statement.KyuubiSQLException")
          .get("count").asInt() == 1)

      val histograms = res2.get("histograms")
      assert(histograms.has("kyuubi.operation.rpc_time.execute_statement"))
      val rpcTimeHistograms = histograms.get("kyuubi.operation.rpc_time.execute_statement")
      assert(rpcTimeHistograms.get("count").asInt() == 3)
      assert(rpcTimeHistograms.get("max").asInt() >= rpcTimeHistograms.get("mean").asInt() &&
          rpcTimeHistograms.get("mean").asInt() >= rpcTimeHistograms.get("min").asInt())
    }
  }
}
