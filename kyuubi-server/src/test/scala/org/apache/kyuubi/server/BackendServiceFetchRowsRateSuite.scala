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

class BackendServiceFetchRowsRateSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = getJdbcUrl

  val reportPath: Path = Utils.createTempDir()
  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(MetricsConf.METRICS_REPORTERS, Seq("JSON"))
      .set(MetricsConf.METRICS_JSON_LOCATION, reportPath.toString)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofMillis(100).toMillis)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    withJdbcStatement() { statement =>
      statement.executeQuery("CREATE TABLE stu_test(id int, name string) USING parquet")
    }
  }

  test("backend service method fetch logs and results rate test") {
    val objMapper = new ObjectMapper()

    withJdbcStatement() { statement =>
      statement.execute("insert into stu_test values(1, 'a'), (2, 'b'), (3, 'c')")
      Thread.sleep(Duration.ofMillis(111).toMillis)
      val res1 = objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)

      assert(res1.has("meters"))
      val meters1 = res1.get("meters")
      val logMeter1 = meters1.get(MetricsConstants.FETCH_LOG_RATE)
      val logRows1 = logMeter1.get("count").asInt()
      assert(logRows1 > 0)

      statement.execute("select * from stu_test limit 2")
      statement.getResultSet.next()
      Thread.sleep(Duration.ofMillis(111).toMillis)

      val meters2 =
        objMapper.readTree(Paths.get(reportPath.toString, "report.json").toFile).get("meters")
      assert(meters2.get(MetricsConstants.FETCH_RESULT_RATE).get("count").asInt() == 2)
      assert(meters2.get(MetricsConstants.FETCH_LOG_RATE).get("count").asInt() >= logRows1)
    }
  }

  override def afterAll(): Unit = {
    withJdbcStatement() { statement =>
      statement.executeQuery("DROP TABLE IF EXISTS stu_test")
    }
    super.afterAll()
  }
}
