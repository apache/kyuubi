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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand
import org.apache.spark.sql.kyuubi.KyuubiSQLConf

class RepartitionBeforeWritingSuite extends KyuubiSparkSQLExtensionTest {
  test("check repartition exists") {
    def check(df: DataFrame): Unit = {
      assert(
        df.queryExecution.analyzed.collect {
          case r: RepartitionByExpression =>
            assert(r.optNumPartitions ===
              spark.sessionState.conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_NUM))
            r
        }.size == 1
      )
    }

    // It's better to set config explicitly in case of we change the default value.
    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true") {
      Seq("USING PARQUET", "").foreach { storage =>
        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          check(sql("INSERT INTO TABLE tmp1 PARTITION(c2='a') " +
            "SELECT * FROM VALUES(1),(2) AS t(c1)"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage")
          check(sql("INSERT INTO TABLE tmp1 SELECT * FROM VALUES(1),(2),(3) AS t(c1)"))
          check(sql("INSERT INTO TABLE tmp1 " +
            "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage AS SELECT * FROM VALUES(1),(2),(3) AS t(c1)")
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage PARTITIONED BY(c2) AS " +
            s"SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)")
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage PARTITIONED BY(c2) AS " +
            s"SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2) DISTRIBUTE BY rand()")
        }
      }
    }
  }

  test("check repartition does not exists") {
    def check(df: DataFrame): Unit = {
      assert(
        df.queryExecution.analyzed.collect {
          case r: RepartitionByExpression => r
        }.isEmpty
      )
    }

    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true") {
      // test no write command
      check(sql("SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))
      check(sql("SELECT count(*) FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))

      // test not supported plan
      withTable("tmp1") {
        sql(s"CREATE TABLE tmp1 (c1 int) PARTITIONED BY (c2 string)")
        check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
          "SELECT /*+ repartition(10) */ * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))
        check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
          "SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2) ORDER BY c1"))
        check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
          "SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2) LIMIT 10"))
      }
    }

    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "false") {
      Seq("USING PARQUET", "").foreach { storage =>
        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
            "SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage")
          check(sql("INSERT INTO TABLE tmp1 SELECT * FROM VALUES(1),(2),(3) AS t(c1)"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage AS SELECT * FROM VALUES(1),(2),(3) AS t(c1)")
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage PARTITIONED BY(c2) AS " +
            s"SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)")
        }
      }
    }
  }

  test("test dynamic partition write") {
    def checkRepartitionExpression(df: DataFrame): Unit = {
      assert(df.queryExecution.analyzed.collect {
        case r: RepartitionByExpression if r.partitionExpressions.size == 2 =>
          assert(r.partitionExpressions.head.asInstanceOf[Attribute].name === "c2")
          assert(r.partitionExpressions(1).asInstanceOf[Cast].child.asInstanceOf[Multiply]
            .left.asInstanceOf[Attribute].name.startsWith("_nondeterministic"))
          r
      }.size == 1)
    }

    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM.key -> "2") {
      Seq("USING PARQUET", "").foreach { storage =>
        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          checkRepartitionExpression(sql("INSERT INTO TABLE tmp1 SELECT 1 as c1, 'a' as c2 "))
        }

        withTable("tmp1") {
          checkRepartitionExpression(
            sql("CREATE TABLE tmp1 PARTITIONED BY(C2) SELECT 1 as c1, 'a' as c2 "))
        }
      }
    }
  }

  test("OptimizedCreateHiveTableAsSelectCommand") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "true",
      HiveUtils.CONVERT_METASTORE_CTAS.key -> "true") {
      withTable("t") {
        val df = sql(s"CREATE TABLE t STORED AS parquet AS SELECT 1 as a")
        val ctas = df.queryExecution.analyzed.collect {
          case _: OptimizedCreateHiveTableAsSelectCommand => true
        }
        assert(ctas.size == 1)
        val repartition = df.queryExecution.analyzed.collect {
          case _: RepartitionByExpression => true
        }
        assert(repartition.size == 1)
      }
    }
  }
}
