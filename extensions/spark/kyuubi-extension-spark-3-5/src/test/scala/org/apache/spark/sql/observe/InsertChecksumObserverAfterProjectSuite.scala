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
package org.apache.spark.sql.observe

import org.apache.spark.sql.{KyuubiSparkSQLExtensionTest, QueryTest, Row}

import org.apache.kyuubi.sql.KyuubiSQLConf.INSERT_CHECKSUM_OBSERVER_AFTER_PROJECT_ENABLED

class InsertChecksumObserverAfterProjectSuite extends KyuubiSparkSQLExtensionTest {

  test("insert checksum observer after project") {
    withSQLConf(INSERT_CHECKSUM_OBSERVER_AFTER_PROJECT_ENABLED.key -> "true") {
      withTable("t") {
        sql("CREATE TABLE t(i int)")
        sql("INSERT INTO t VALUES (1), (2), (3)")
        val df = sql("select a from (SELECT i as a FROM t) where a > 1")
        df.collect()
        val metrics = df.queryExecution.observedMetrics
        assert(metrics.size == 2)
        QueryTest.sameRows(
          Seq(Row(BigDecimal(6569872598L), 2), Row(BigDecimal(8017165408L), 3)),
          metrics.values.toSeq)
      }
    }
  }

}
