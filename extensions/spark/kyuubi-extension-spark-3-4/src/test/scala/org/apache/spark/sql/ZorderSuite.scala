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

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{RebalancePartitions, Sort}
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.{KyuubiSQLConf, SparkKyuubiSparkSQLParser}
import org.apache.kyuubi.sql.zorder.Zorder

trait ZorderSuiteSpark extends ZorderSuiteBase {

  test("Add rebalance before zorder") {
    Seq("true" -> false, "false" -> true).foreach { case (useOriginalOrdering, zorder) =>
      withSQLConf(
        KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED.key -> "false",
        KyuubiSQLConf.REBALANCE_BEFORE_ZORDER.key -> "true",
        KyuubiSQLConf.REBALANCE_ZORDER_COLUMNS_ENABLED.key -> "true",
        KyuubiSQLConf.ZORDER_USING_ORIGINAL_ORDERING_ENABLED.key -> useOriginalOrdering) {
        withTable("t") {
          sql(
            """
              |CREATE TABLE t (c1 int, c2 string) PARTITIONED BY (d string)
              | TBLPROPERTIES (
              |'kyuubi.zorder.enabled'= 'true',
              |'kyuubi.zorder.cols'= 'c1,C2')
              |""".stripMargin)
          val p = sql("INSERT INTO TABLE t PARTITION(d='a') SELECT * FROM VALUES(1,'a')")
            .queryExecution.analyzed
          assert(p.collect {
            case sort: Sort
                if !sort.global &&
                  ((sort.order.exists(_.child.isInstanceOf[Zorder]) && zorder) ||
                    (!sort.order.exists(_.child.isInstanceOf[Zorder]) && !zorder)) => sort
          }.size == 1)
          assert(p.collect {
            case rebalance: RebalancePartitions
                if rebalance.references.map(_.name).exists(_.equals("c1")) => rebalance
          }.size == 1)

          val p2 = sql("INSERT INTO TABLE t PARTITION(d) SELECT * FROM VALUES(1,'a','b')")
            .queryExecution.analyzed
          assert(p2.collect {
            case sort: Sort
                if (!sort.global && Seq("c1", "c2", "d").forall(x =>
                  sort.references.map(_.name).exists(_.equals(x)))) &&
                  ((sort.order.exists(_.child.isInstanceOf[Zorder]) && zorder) ||
                    (!sort.order.exists(_.child.isInstanceOf[Zorder]) && !zorder)) => sort
          }.size == 1)
          assert(p2.collect {
            case rebalance: RebalancePartitions
                if Seq("c1", "c2", "d").forall(x =>
                  rebalance.references.map(_.name).exists(_.equals(x))) => rebalance
          }.size == 1)
        }
      }
    }
  }

  test("Two phase rebalance before Z-Order") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.CollapseRepartition",
      KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED.key -> "false",
      KyuubiSQLConf.REBALANCE_BEFORE_ZORDER.key -> "true",
      KyuubiSQLConf.TWO_PHASE_REBALANCE_BEFORE_ZORDER.key -> "true",
      KyuubiSQLConf.REBALANCE_ZORDER_COLUMNS_ENABLED.key -> "true") {
      withTable("t") {
        sql(
          """
            |CREATE TABLE t (c1 int) PARTITIONED BY (d string)
            | TBLPROPERTIES (
            |'kyuubi.zorder.enabled'= 'true',
            |'kyuubi.zorder.cols'= 'c1')
            |""".stripMargin)
        val p = sql("INSERT INTO TABLE t PARTITION(d) SELECT * FROM VALUES(1,'a')")
        val rebalance = p.queryExecution.optimizedPlan.innerChildren
          .flatMap(_.collect { case r: RebalancePartitions => r })
        assert(rebalance.size == 2)
        assert(rebalance.head.partitionExpressions.flatMap(_.references.map(_.name))
          .contains("d"))
        assert(rebalance.head.partitionExpressions.flatMap(_.references.map(_.name))
          .contains("c1"))

        assert(rebalance(1).partitionExpressions.flatMap(_.references.map(_.name))
          .contains("d"))
        assert(!rebalance(1).partitionExpressions.flatMap(_.references.map(_.name))
          .contains("c1"))
      }
    }
  }
}

trait ParserSuite { self: ZorderSuiteBase =>
  override def createParser: ParserInterface = {
    new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
  }
}

class ZorderWithCodegenEnabledSuite
  extends ZorderWithCodegenEnabledSuiteBase
  with ZorderSuiteSpark
  with ParserSuite {}
class ZorderWithCodegenDisabledSuite
  extends ZorderWithCodegenDisabledSuiteBase
  with ZorderSuiteSpark
  with ParserSuite {}
