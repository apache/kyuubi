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

import org.apache.kyuubi.sql.KyuubiSQLConf
import org.apache.kyuubi.sql.watchdog.{DangerousJoinCounter, KyuubiDangerousJoinException}

class KyuubiDangerousJoinIT extends KyuubiSparkSQLExtensionTest {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  test("warn action should keep query successful and emit warning diagnostics") {
    DangerousJoinCounter.reset()
    withSQLConf(
      KyuubiSQLConf.DANGEROUS_JOIN_ENABLED.key -> "true",
      KyuubiSQLConf.DANGEROUS_JOIN_ACTION.key -> "WARN",
      "spark.sql.autoBroadcastJoinThreshold" -> "1") {
      val rows = sql("SELECT count(*) FROM t1 a JOIN t2 b ON a.c1 = b.c1").collect()
      assert(rows.nonEmpty)
      assert(DangerousJoinCounter.count >= 1)
      assert(DangerousJoinCounter.latest.exists(_.toJson.contains("joinType")))
    }
  }

  test("reject action should return detailed dangerous join exception") {
    DangerousJoinCounter.reset()
    withSQLConf(
      KyuubiSQLConf.DANGEROUS_JOIN_ENABLED.key -> "true",
      KyuubiSQLConf.DANGEROUS_JOIN_ACTION.key -> "REJECT",
      "spark.sql.autoBroadcastJoinThreshold" -> "1") {
      val e = intercept[KyuubiDangerousJoinException] {
        sql("SELECT * FROM t1 a JOIN t2 b ON a.c1 > b.c1").collect()
      }
      assert(e.getErrorCode == 41101)
      assert(e.getMessage.contains("leftSize"))
      assert(e.getMessage.contains("rightSize"))
    }
  }
}
