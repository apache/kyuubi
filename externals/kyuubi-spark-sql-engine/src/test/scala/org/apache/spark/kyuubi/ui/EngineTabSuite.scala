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
import org.apache.spark.SparkContext

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.JDBCTestHelper

class EngineTabSuite extends WithSparkSQLEngine with JDBCTestHelper {
  override def withKyuubiConf: Map[String, String] = Map(
    "spark.ui.enabled" -> "true",
    "spark.ui.port" -> "0")

  override def beforeAll(): Unit = {
    SparkContext.getActive.foreach(_.stop())
    super.beforeAll()
  }

  test("basic stats for engine tab") {
    assert(spark.sparkContext.ui.nonEmpty)
    val client = HttpClients.createDefault()
    val req = new HttpGet(spark.sparkContext.uiWebUrl.get + "/kyuubi/")
    val response = client.execute(req)
    assert(response.getStatusLine.getStatusCode === 200)
    val resp = EntityUtils.toString(response.getEntity)
    assert(resp.contains("<strong>Background execution pool threads alive: </strong>"))
    assert(resp.contains("0 session(s) are online,"))
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

  test("session stats for engine tab") {
    assert(spark.sparkContext.ui.nonEmpty)
    val client = HttpClients.createDefault()
    val req = new HttpGet(spark.sparkContext.uiWebUrl.get + "/kyuubi/")
    val response = client.execute(req)
    assert(response.getStatusLine.getStatusCode === 200)
    val resp = EntityUtils.toString(response.getEntity)
    assert(resp.contains("0 session(s) are online,"))
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

      // check session section
      assert(resp.contains("Session Statistics"))

      // check session stats table id
      assert(resp.contains("sessionstat"))

      // check session stats table title
      assert(resp.contains("Total Statements"))
    }
  }

  test("statement stats for engine tab") {
    assert(spark.sparkContext.ui.nonEmpty)
    val client = HttpClients.createDefault()
    val req = new HttpGet(spark.sparkContext.uiWebUrl.get + "/kyuubi/")
    val response = client.execute(req)
    assert(response.getStatusLine.getStatusCode === 200)
    val resp = EntityUtils.toString(response.getEntity)
    assert(resp.contains("0 session(s) are online,"))
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

      // check session section
      assert(resp.contains("SQL Statistics"))

      // check sql stats table id
      assert(resp.contains("sqlstat"))

      // check sql stats table title
      assert(resp.contains("Query Execution"))
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
