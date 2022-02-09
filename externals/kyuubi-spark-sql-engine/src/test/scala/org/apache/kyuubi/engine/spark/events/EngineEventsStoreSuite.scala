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

package org.apache.kyuubi.engine.spark.events

import org.apache.spark.kyuubi.SparkContextHelper

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class EngineEventsStoreSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  var store: EngineEventsStore = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    startSparkEngine()
    val kvStore = SparkContextHelper.getKvStore(spark.sparkContext)
    store = new EngineEventsStore(kvStore)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    stopSparkEngine()
  }

  test("EngineEventsStore session test") {
    assert(store.getSessionList.isEmpty)
    assert(store.getSessionCount == 0)
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
    }
    assert(store.getSessionList.size == 1)
    assert(store.getSessionCount == 1)
  }

  test("EngineEventsStore statement test") {
    assert(store.getStatementList.isEmpty)
    assert(store.getStatementCount == 0)
    val sql = """
                |SELECT
                |  l.id % 100 k,
                |  sum(l.id) sum,
                |  count(l.id) cnt,
                |  avg(l.id) avg,
                |  min(l.id) min,
                |  max(l.id) max
                |from range(0, 100000L, 1, 100) l
                |  left join range(0, 100000L, 2, 100) r ON l.id = r.id
                |GROUP BY 1""".stripMargin
    withJdbcStatement() { statement =>
      statement.execute(sql)
    }
    val statementList = store.getStatementList
    assert(statementList.size == 1)
    assert(store.getStatementCount == 1)
    val statementId = statementList(0).statementId
    assert(store.getStatement(statementId).get.statement === sql)
  }

  override def withKyuubiConf: Map[String, String] = Map.empty

  override protected def jdbcUrl: String = getJdbcUrl
}
