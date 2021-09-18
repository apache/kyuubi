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
import org.apache.kyuubi.sql.watchdog.MaxHivePartitionExceedException

class MaxHivePartitionStrategyTest extends KyuubiSparkSQLExtensionTest {
  test("test watchdog with scan maxHivePartitions") {
    withTable("test", "temp") {
      sql(
        s"""
           |CREATE TABLE test(i int)
           |PARTITIONED BY (p int)
           |STORED AS textfile""".stripMargin)
      spark.range(0, 10, 1).selectExpr("id as col")
        .createOrReplaceTempView("temp")

      for (part <- Range(0, 10)) {
        sql(
          s"""
             |INSERT OVERWRITE TABLE test PARTITION (p='$part')
             |select col from temp""".stripMargin)
      }

      withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_HIVEPARTITION.key -> "5") {

        sql("SELECT * FROM test where p=1").queryExecution.sparkPlan

        sql(
          s"SELECT * FROM test WHERE p in (${Range(0, 5).toList.mkString(",")})")
          .queryExecution.sparkPlan

        intercept[MaxHivePartitionExceedException](
          sql("SELECT * FROM test").queryExecution.sparkPlan)

        intercept[MaxHivePartitionExceedException](sql(
          s"SELECT * FROM test WHERE p in (${Range(0, 6).toList.mkString(",")})")
          .queryExecution.sparkPlan)

      }
    }
  }
}
