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

import org.apache.spark.sql.catalyst.plans.{Inner, LeftExistence}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Filter, Generate, GlobalLimit, Join, LocalLimit, LogicalPlan, Offset, Project, RebalancePartitions, Sample, Sort, UnaryNode, Union, WindowGroupLimit}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ExchangeQueryStageExec, LogicalQueryStage, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.WriteFiles
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.KyuubiSQLConf._

/**
 * An AQE runtime-optimizer rule (injected via `injectRuntimeOptimizerRule`) that removes the
 * `RebalancePartitions` operator injected before writing when the materialized rebalance input
 * makes the rebalance shuffle not worthwhile. Because it runs in the AQE optimizer on the logical
 * plan, the removal is a plain logical rewrite (replace `RebalancePartitions` with its child) and
 * the physical planner re-plans without the rebalance shuffle.
 *
 * Only the rebalance that feeds the final write is considered: starting from the [[WriteFiles]]
 * node, the rule walks down through at most [[Project]] and local [[Sort]] operators to reach the
 * `RebalancePartitions`. Any other operator in between means the rebalance does not directly feed
 * the write, so it is left untouched. A `RebalancePartitions` is only removed when:
 *   1. [[KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED]] is on;
 *   2. it has no partition expressions (i.e. its physical `shuffleOrigin` would be
 *      `REBALANCE_PARTITIONS_BY_NONE`);
 *   3. its `optAdvisoryPartitionSize` is present and larger than the session
 *      `spark.sql.adaptive.advisoryPartitionSizeInBytes` (otherwise the smaller advisory size
 *      suggests the user intentionally wants to balance data into smaller partitions).
 *
 * The rebalance input is split into independent "query stage groups" by
 * [[collectQueryStageGroupSize]]: only sub-plans whose leaves are all materialized
 * [[LogicalQueryStage]] exchange stages contribute a group of stage sizes; a `Union` produces one
 * group per branch. The size used per stage is its runtime `sizeInBytes`. Every group must be
 * removable for the shuffle to be removed. A group is removable when either:
 *   - Large data: the rebalance input has no data-reducing operator and the group's representative
 *     size (`max(maxStageSize, groupSum / 2)`) is larger than
 *     `numShufflePartitions * [[KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_SMALL_PARTITION_SIZE]]`; or
 *   - Small data: the rebalance input has no data-expanding operator and the representative size is
 *     smaller than
 *     `advisoryPartitionSize * [[KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_TOLERABLE_SMALL_FILE_NUM]]`.
 */
case class RemoveRebalanceShuffle(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(REMOVE_REBALANCE_SHUFFLE_ENABLED)) {
      return plan
    }

    plan.transformDown {
      case write: WriteFiles => write.withNewChildren(Seq(removeRebalance(write.child)))
    }
  }

  /**
   * Walk down from the write through at most [[Project]] and local [[Sort]] operators to find the
   * `RebalancePartitions` that feeds the write, and remove it (replace with its child) if eligible.
   * Any other operator in between means the rebalance does not directly feed the write, so it is
   * left untouched.
   */
  private def removeRebalance(plan: LogicalPlan): LogicalPlan = plan match {
    case p: Project => p.withNewChildren(Seq(removeRebalance(p.child)))
    case s: Sort if !s.global => s.withNewChildren(Seq(removeRebalance(s.child)))
    case RebalancePartitions(partitionExpressions, child, _, optAdvisoryPartitionSize)
        if partitionExpressions.isEmpty && optAdvisoryPartitionSize.isDefined &&
          shouldRemove(child, optAdvisoryPartitionSize.get) =>
      child
    case other => other
  }

  /**
   * Collect the runtime sizes of the materialized query stages below the rebalance, grouped so
   * that stages that must be considered together stay in one group. A sub-plan contributes a group
   * only when all of its leaves are [[LogicalQueryStage]] exchange stages and every such stage is
   * materialized (a broadcast stage needs `isMaterialized`; a shuffle stage additionally needs
   * `mapStats`); otherwise the sub-plan is skipped. A [[Union]] yields one group per child so the
   * branches are evaluated independently. Returns an empty sequence when no group is found.
   */
  private def collectQueryStageGroupSize(plan: LogicalPlan): Seq[Seq[Long]] = plan match {
    case u: UnaryNode => collectQueryStageGroupSize(u.child)
    case u: Union => u.children.flatMap(collectQueryStageGroupSize)
    case p if p.collectLeaves().forall(_.isInstanceOf[LogicalQueryStage]) =>
      val stages = p.collect {
        case LogicalQueryStage(_, stage: ExchangeQueryStageExec) => stage
      }
      val allValid = stages.forall {
        case s: BroadcastQueryStageExec => s.isMaterialized
        case s: ShuffleQueryStageExec => s.isMaterialized && s.mapStats.isDefined
        case _ => false
      }
      if (allValid) {
        Seq(stages.map(_.getRuntimeStatistics.sizeInBytes.min(Long.MaxValue).toLong))
      } else {
        Seq.empty
      }
    case _ => Seq.empty
  }

  /**
   * Decide whether the rebalance feeding the write can be removed. The rebalance advisory size must
   * be larger than the session advisory size, every query stage group below the rebalance must be
   * materialized, and every group must be removable under either the large-data (no data-reducing
   * operator) or the small-data (no data-expanding operator) condition.
   */
  private def shouldRemove(child: LogicalPlan, rebalanceAdvisorySize: Long): Boolean = {
    val sessionAdvisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    // Maybe intentionally by user to balance data
    if (rebalanceAdvisorySize <= sessionAdvisorySize) {
      logInfo(s"Keep rebalance shuffle since the rebalance advisory partition size " +
        s"($rebalanceAdvisorySize bytes) is not larger than the session advisory partition size " +
        s"($sessionAdvisorySize bytes).")
      return false
    }

    val groupedStageSize = collectQueryStageGroupSize(child)
    if (groupedStageSize.isEmpty) {
      logInfo("Keep rebalance shuffle since no materialized query stage group is found below " +
        "the rebalance.")
      return false
    }

    val shufflePartitions = conf.numShufflePartitions
    val smallPartitionSizeThreshold = conf.getConf(REMOVE_REBALANCE_SHUFFLE_SMALL_PARTITION_SIZE)
    val minAllowStageSize = shufflePartitions * smallPartitionSizeThreshold
    val hasReducingOperators = hasReducingOperator(child)
    val hasExpandingOperators = hasExpandingOperator(child)
    val tolerableSmallFileNum = conf.getConf(REMOVE_REBALANCE_SHUFFLE_TOLERABLE_SMALL_FILE_NUM)

    def shouldRemoveOneGroup(stageSizes: Seq[Long]): Boolean = {
      val maxStageSize = stageSizes.max.max(stageSizes.sum / 2)
      (!hasReducingOperators && maxStageSize > minAllowStageSize) ||
      (!hasExpandingOperators && maxStageSize < sessionAdvisorySize * tolerableSmallFileNum)
    }

    val remove = groupedStageSize.forall(shouldRemoveOneGroup)
    logInfo(s"${if (remove) "Remove" else "Keep"} rebalance shuffle. " +
      s"rebalanceAdvisorySize=$rebalanceAdvisorySize, sessionAdvisorySize=$sessionAdvisorySize, " +
      s"shufflePartitions=$shufflePartitions, smallPartitionSize=$smallPartitionSizeThreshold, " +
      s"minAllowStageSize=$minAllowStageSize, tolerableSmallFileNum=$tolerableSmallFileNum, " +
      s"hasReducingOperators=$hasReducingOperators, hasExpandingOperators=$hasExpandingOperators, " +
      s"groupedStageSize=${groupedStageSize.map(_.mkString("[", ",", "]")).mkString(", ")}.")
    remove
  }

  /** Whether the rebalance input contains an operator that may reduce the data size. */
  private def hasReducingOperator(plan: LogicalPlan): Boolean = {
    plan.exists {
      case _: Aggregate => true
      case _: Filter => true
      case _: Sample => true
      case _: Offset => true
      case _: GlobalLimit => true
      case _: LocalLimit => true
      case _: WindowGroupLimit => true
      case Join(_, _, joinType, _, _) =>
        joinType match {
          case LeftExistence(_) => true
          case Inner => true
          case _ => false
        }
      case _ => false
    }
  }

  /** Whether the rebalance input contains an operator that may expand the data size. */
  private def hasExpandingOperator(plan: LogicalPlan): Boolean = {
    plan.exists {
      case _: Generate => true
      case _: Expand => true
      case Join(_, _, LeftExistence(_), _, _) => false
      case _: Join => true
      case _ => false
    }
  }
}
