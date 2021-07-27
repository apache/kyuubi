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

package org.apache.spark.kyuubi.ui

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.JDBCTestUtils

class EngineTabSuite extends WithSparkSQLEngine with JDBCTestUtils {
  override def withKyuubiConf: Map[String, String] = Map(
    "spark.ui.enabled" -> "true",
    "spark.ui.port" -> "0")

  private lazy val tab = EngineTab(engine)
  private lazy val req = new HttpGet(spark.sparkContext.uiWebUrl.get + "/" + tab.prefix)
  private lazy val client = HttpClients.createDefault()

  test("basic stats for engine tab") {
    assert(tab.name === "Kyuubi Query Engine")
    assert(tab.prefix === "kyuubi")
    assert(tab.pages.size === 1)
    val enginePage = tab.pages(0)
    assert(enginePage.isInstanceOf[EnginePage])
    val response = client.execute(req)
    assert(response.getStatusLine.getStatusCode === 200)
    val resp = EntityUtils.toString(response.getEntity)
    assert(resp.contains("<strong>Background execution pool threads alive: </strong>"))
  }

  test("active sessions update when there are connections") {
    withJdbcStatement() { statement =>
      statement.execute(
        """
          |SELECT
          |  l.id % 100 k,
          |  sum(l.id) sum,
          |  count(l.id) cnt,
          |  avg(l.id) avg,
          |  min(l.id) min,
          |  max(l.id) max
          |from range(0, 100000L, 1, 100) l
          |  left join range(0, 100000L, 2, 100) r ON l.id = r.id
          |GROUP BY 1""".stripMargin)
      val response = client.execute(req)
      assert(response.getStatusLine.getStatusCode === 200)
      val resp = EntityUtils.toString(response.getEntity)
      assert(resp.contains("1 session(s) are online,"))
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
