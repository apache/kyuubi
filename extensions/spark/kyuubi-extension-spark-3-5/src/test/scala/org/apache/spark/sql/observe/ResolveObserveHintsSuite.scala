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

import org.apache.kyuubi.sql.KyuubiSQLConf.OBSERVE_HINT_ENABLE

class ResolveObserveHintsSuite extends KyuubiSparkSQLExtensionTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  test("test observe hint") {
    withSQLConf(OBSERVE_HINT_ENABLE.key -> "true") {
      val sqlText =
        s"""
           | SELECT /*+ OBSERVE('observer3', sum(tt2.c3), count(1)) */ *
           |  FROM
           | (SELECT /*+ OBSERVE('observer1', sum(c1), count(1)) */ * from t1) tt1
           |  join
           | (SELECT /*+ OBSERVE('observer2', sum(c1), count(1)) */ c1, c1 * 2 as c3 from t2) tt2
           |  on tt1.c1 = tt2.c1
           |""".stripMargin
      val df = spark.sql(sqlText)
      df.collect()
      val observedMetrics = df.queryExecution.observedMetrics
      assert(observedMetrics.size == 3)
      QueryTest.sameRows(Seq(observedMetrics("observer1")), Seq(Row(5050, 100)))
      QueryTest.sameRows(Seq(observedMetrics("observer2")), Seq(Row(55, 10)))
      QueryTest.sameRows(Seq(observedMetrics("observer3")), Seq(Row(110, 10)))
    }
  }
}
