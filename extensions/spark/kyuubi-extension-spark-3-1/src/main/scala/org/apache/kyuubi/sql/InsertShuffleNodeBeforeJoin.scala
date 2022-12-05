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

package org.apache.kyuubi.sql

import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.KyuubiSQLConf._

/**
 * Insert shuffle node before join if it doesn't exist to make `OptimizeSkewedJoin` works.
 */
object InsertShuffleNodeBeforeJoin extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    // this rule has no meaning without AQE
    if (!conf.getConf(FORCE_SHUFFLE_BEFORE_JOIN) ||
      !conf.getConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)) {
      return plan
    }

    val newPlan = insertShuffleBeforeJoin(plan)
    if (plan.fastEquals(newPlan)) {
      plan
    } else {
      // make sure the output partitioning and ordering will not be broken.
      KyuubiEnsureRequirements.apply(newPlan)
    }
  }

  private def insertShuffleBeforeJoin(plan: SparkPlan): SparkPlan = plan transformUp {
    case smj @ SortMergeJoinExec(_, _, _, _, l, r, _) =>
      smj.withNewChildren(checkAndInsertShuffle(smj.requiredChildDistribution.head, l) ::
        checkAndInsertShuffle(smj.requiredChildDistribution(1), r) :: Nil)

    case shj: ShuffledHashJoinExec =>
      if (!shj.left.isInstanceOf[Exchange] && !shj.right.isInstanceOf[Exchange]) {
        shj.withNewChildren(withShuffleExec(shj.requiredChildDistribution.head, shj.left) ::
          withShuffleExec(shj.requiredChildDistribution(1), shj.right) :: Nil)
      } else if (!shj.left.isInstanceOf[Exchange]) {
        shj.withNewChildren(
          withShuffleExec(shj.requiredChildDistribution.head, shj.left) :: shj.right :: Nil)
      } else if (!shj.right.isInstanceOf[Exchange]) {
        shj.withNewChildren(
          shj.left :: withShuffleExec(shj.requiredChildDistribution(1), shj.right) :: Nil)
      } else {
        shj
      }
  }

  private def checkAndInsertShuffle(
      distribution: Distribution,
      child: SparkPlan): SparkPlan = child match {
    case SortExec(_, _, _: Exchange, _) =>
      child
    case SortExec(_, _, _: QueryStageExec, _) =>
      child
    case sort @ SortExec(_, _, agg: BaseAggregateExec, _) =>
      sort.withNewChildren(withShuffleExec(distribution, agg) :: Nil)
    case _ =>
      withShuffleExec(distribution, child)
  }

  private def withShuffleExec(distribution: Distribution, child: SparkPlan): SparkPlan = {
    val numPartitions = distribution.requiredNumPartitions
      .getOrElse(conf.numShufflePartitions)
    ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child)
  }
}
