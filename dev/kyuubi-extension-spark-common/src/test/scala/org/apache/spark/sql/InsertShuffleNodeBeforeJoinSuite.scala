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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeLike}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import org.apache.kyuubi.sql.KyuubiSQLConf

class InsertShuffleNodeBeforeJoinSuite extends KyuubiSparkSQLExtensionTest {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLCommonExtension")
  }

  test("force shuffle before join") {
    def checkShuffleNodeNum(sqlString: String, num: Int): Unit = {
      var expectedResult: Seq[Row] = Seq.empty
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        expectedResult = sql(sqlString).collect()
      }
      val df = sql(sqlString)
      checkAnswer(df, expectedResult)
      assert(
        collect(df.queryExecution.executedPlan) {
          case shuffle: ShuffleExchangeLike
            if shuffle.shuffleOrigin == ENSURE_REQUIREMENTS => shuffle
        }.size == num)
    }

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      KyuubiSQLConf.FORCE_SHUFFLE_BEFORE_JOIN.key -> "true") {
      Seq("SHUFFLE_HASH", "MERGE").foreach { joinHint =>
        // positive case
        checkShuffleNodeNum(
          s"""
             |SELECT /*+ $joinHint(t2, t3) */ t1.c1, t1.c2, t2.c1, t3.c1 from t1
             | JOIN t2 ON t1.c1 = t2.c1
             | JOIN t3 ON t1.c1 = t3.c1
             | """.stripMargin, 4)

        // negative case
        checkShuffleNodeNum(
          s"""
             |SELECT /*+ $joinHint(t2, t3) */ t1.c1, t1.c2, t2.c1, t3.c1 from t1
             | JOIN t2 ON t1.c1 = t2.c1
             | JOIN t3 ON t1.c2 = t3.c2
             | """.stripMargin, 4)
      }

      checkShuffleNodeNum(
        """
          |SELECT t1.c1, t2.c1, t3.c2 from t1
          | JOIN t2 ON t1.c1 = t2.c1
          | JOIN (
          |  SELECT c2, count(*) FROM t1 GROUP BY c2
          | ) t3 ON t1.c1 = t3.c2
          | """.stripMargin, 5)

      checkShuffleNodeNum(
        """
          |SELECT t1.c1, t2.c1, t3.c1 from t1
          | JOIN t2 ON t1.c1 = t2.c1
          | JOIN (
          |  SELECT c1, count(*) FROM t1 GROUP BY c1
          | ) t3 ON t1.c1 = t3.c1
          | """.stripMargin, 5)
    }
  }
}
