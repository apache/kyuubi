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

package org.apache.spark.sql.kyuubi

import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

class SparkDatasetHelperSuite extends WithSparkSQLEngine {
  override def withKyuubiConf: Map[String, String] = Map.empty

  test("get limit from spark plan") {
    Seq(true, false).foreach { aqe =>
      val topKThreshold = 3
      spark.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, aqe)
      spark.sessionState.conf.setConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD, topKThreshold)
      spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv AS" +
        " SELECT * FROM VALUES(1),(2),(3),(4) AS t(id)")

      val topKStatement = s"SELECT * FROM(SELECT * FROM tv ORDER BY id LIMIT ${topKThreshold - 1})"
      assert(SparkDatasetHelper.optimizedPlanLimit(
        spark.sql(topKStatement).queryExecution) === Option(topKThreshold - 1))

      val collectLimitStatement =
        s"SELECT * FROM (SELECT * FROM tv ORDER BY id LIMIT $topKThreshold)"
      assert(SparkDatasetHelper.optimizedPlanLimit(
        spark.sql(collectLimitStatement).queryExecution) === Option(topKThreshold))
    }
  }

  test("isCommandExec") {
    var query = "set"
    assert(SparkDatasetHelper.isCommandExec(spark.sql(query)))
    query = "explain set"
    assert(SparkDatasetHelper.isCommandExec(spark.sql(query)))
    query = "show tables"
    assert(SparkDatasetHelper.isCommandExec(spark.sql(query)))
    query = "select * from VALUES(1),(2),(3),(4) AS t(id)"
    assert(!SparkDatasetHelper.isCommandExec(spark.sql(query)))
  }
}
