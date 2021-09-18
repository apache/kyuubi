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

import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit

import org.apache.kyuubi.sql.KyuubiSQLConf
import org.apache.kyuubi.sql.watchdog.MaxHivePartitionExceedException

class WatchDogTest extends KyuubiSparkSQLExtensionTest {
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

  test("test watchdog with query forceMaxOutputRows") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      assert(sql("SELECT * FROM t1")
        .queryExecution.analyzed.isInstanceOf[GlobalLimit])

      assert(sql("SELECT * FROM t1 LIMIT 1")
        .queryExecution.analyzed.asInstanceOf[GlobalLimit].maxRows.contains(1))

      assert(sql("SELECT * FROM t1 LIMIT 11")
        .queryExecution.analyzed.asInstanceOf[GlobalLimit].maxRows.contains(10))

      assert(!sql("SELECT count(*) FROM t1")
        .queryExecution.analyzed.isInstanceOf[GlobalLimit])

      assert(sql(
        """
          |SELECT c1, COUNT(*)
          |FROM t1
          |GROUP BY c1
          |""".stripMargin).queryExecution.analyzed.isInstanceOf[GlobalLimit])

      assert(sql(
        """
          |WITH custom_cte AS (
          |SELECT * FROM t1
          |)
          |
          |SELECT * FROM custom_cte
          |""".stripMargin).queryExecution
        .analyzed.isInstanceOf[GlobalLimit])

      assert(sql(
        """
          |WITH custom_cte AS (
          |SELECT * FROM t1
          |)
          |
          |SELECT * FROM custom_cte
          |LIMIT 1
          |""".stripMargin).queryExecution
        .analyzed.asInstanceOf[GlobalLimit].maxRows.contains(1))

      assert(sql(
        """
          |WITH custom_cte AS (
          |SELECT * FROM t1
          |)
          |
          |SELECT * FROM custom_cte
          |LIMIT 11
          |""".stripMargin).queryExecution
        .analyzed.asInstanceOf[GlobalLimit].maxRows.contains(10))

      assert(!sql(
        """
          |WITH custom_cte AS (
          |SELECT * FROM t1
          |)
          |
          |SELECT COUNT(*) FROM custom_cte
          |""".stripMargin).queryExecution
        .analyzed.isInstanceOf[GlobalLimit])

      assert(sql(
        """
          |WITH custom_cte AS (
          |SELECT * FROM t1
          |)
          |
          |SELECT c1, COUNT(*)
          |FROM custom_cte
          |GROUP BY c1
          |""".stripMargin).queryExecution
        .analyzed.isInstanceOf[GlobalLimit])

      assert(sql(
        """
          |WITH custom_cte AS (
          |SELECT * FROM t1
          |)
          |
          |SELECT c1, COUNT(*)
          |FROM custom_cte
          |GROUP BY c1
          |LIMIT 11
          |""".stripMargin).queryExecution
        .analyzed.asInstanceOf[GlobalLimit].maxRows.contains(10))
    }
  }
}
