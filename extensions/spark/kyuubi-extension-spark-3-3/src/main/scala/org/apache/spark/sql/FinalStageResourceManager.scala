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

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{ExecutorAllocationClient, MapOutputTrackerMaster, SparkContext, SparkEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}

import org.apache.kyuubi.sql.{KyuubiSQLConf, MarkNumOutputColumnsRule}

/**
 * This rule assumes the final write stage has less cores requirement than previous, otherwise
 * this rule would take no effect.
 *
 * It provide a feature:
 * 1. Kill redundant executors before running final write stage
 */
case class FinalStageResourceManager(session: SparkSession)
  extends Rule[SparkPlan] with FinalRebalanceStageHelper {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(KyuubiSQLConf.FINAL_WRITE_STAGE_EAGERLY_KILL_EXECUTORS_ENABLED)) {
      return plan
    }

    if (!MarkNumOutputColumnsRule.isWrite(session, plan)) {
      return plan
    }

    val sc = session.sparkContext
    val dra = sc.getConf.getBoolean("spark.dynamicAllocation.enabled", false)
    val coresPerExecutor = sc.getConf.getInt("spark.executor.cores", 1)
    val minExecutors = sc.getConf.getInt("spark.dynamicAllocation.minExecutors", 0)
    val maxExecutors = sc.getConf.getInt("spark.dynamicAllocation.maxExecutors", Int.MaxValue)
    val factor = conf.getConf(KyuubiSQLConf.FINAL_WRITE_STAGE_PARTITION_FACTOR)
    val hasImprovementRoom = maxExecutors - 1 > minExecutors * factor
    // Fast fail if:
    // 1. DRA off
    // 2. only work with yarn and k8s
    // 3. maxExecutors is not bigger than minExecutors * factor
    if (!dra || !sc.schedulerBackend.isInstanceOf[CoarseGrainedSchedulerBackend] ||
      !hasImprovementRoom) {
      return plan
    }

    val stage = findFinalRebalanceStage(plan)
    if (stage.isEmpty) {
      return plan
    }

    // Since we are in `prepareQueryStage`, the AQE shuffle read has not been applied.
    // So we need to apply it by self.
    val shuffleRead = queryStageOptimizerRules.foldLeft(stage.get.asInstanceOf[SparkPlan]) {
      case (latest, rule) => rule.apply(latest)
    }
    shuffleRead match {
      case AQEShuffleReadExec(stage: ShuffleQueryStageExec, partitionSpecs) =>
        // The condition whether inject custom resource profile:
        // - target executors < active executors
        // - active executors - target executors > min executors
        val numActiveExecutors = sc.getExecutorIds().length
        val targetCores = partitionSpecs.length
        val targetExecutors = (math.ceil(targetCores.toFloat / coresPerExecutor) * factor).toInt
          .max(1)
        val hasBenefits = targetExecutors < numActiveExecutors &&
          (numActiveExecutors - targetExecutors) > minExecutors
        if (hasBenefits) {
          val shuffleId = stage.plan.asInstanceOf[ShuffleExchangeExec].shuffleDependency.shuffleId
          val numReduce = stage.plan.asInstanceOf[ShuffleExchangeExec].numPartitions
          // Now, there is only a final rebalance stage waiting to execute and all tasks of previous
          // stage are finished. Kill redundant existed executors eagerly so the tasks of final
          // stage can be centralized scheduled.
          killExecutors(sc, targetExecutors, shuffleId, numReduce)
        } else {
          logInfo(s"Has no benefits to kill executors or inject custom resource profile, " +
            s"active executors: $numActiveExecutors, min executor: $minExecutors, " +
            s"target executors: $targetExecutors.")
        }

      case _ =>
    }

    plan
  }

  /**
   * The priority of kill executors follow:
   * 1. kill executor who is younger than other (The older the JIT works better)
   * 2. kill executor who produces less shuffle data first
   */
  private def findExecutorToKill(
      sc: SparkContext,
      targetExecutors: Int,
      shuffleId: Int,
      numReduce: Int): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val shuffleStatus = tracker.shuffleStatuses(shuffleId)
    val executorToBlockSize = new mutable.HashMap[String, Long]
    shuffleStatus.withMapStatuses { mapStatus =>
      mapStatus.foreach { status =>
        var i = 0
        var sum = 0L
        while (i < numReduce) {
          sum += status.getSizeForBlock(i)
          i += 1
        }
        executorToBlockSize.getOrElseUpdate(status.location.executorId, sum)
      }
    }

    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val executorsWithRegistrationTs = backend.getExecutorsWithRegistrationTs()
    val existedExecutors = executorsWithRegistrationTs.keys.toSet
    val expectedNumExecutorToKill = existedExecutors.size - targetExecutors
    if (expectedNumExecutorToKill < 1) {
      return Seq.empty
    }

    val executorIdsToKill = new ArrayBuffer[String]()
    // We first kill executor who does not hold shuffle block. It would happen because
    // the last stage is running fast and finished in a short time. The existed executors are
    // from previous stages that have not been killed by DRA, so we can not find it by tracking
    // shuffle status.
    // We should evict executors by their alive time first and retain all of executors which
    // have better locality for shuffle block.
    val numExecutorToKillWithNoShuffle = expectedNumExecutorToKill - executorToBlockSize.size
    executorsWithRegistrationTs.toSeq.sortBy(_._2).foreach { case (id, _) =>
      if (executorIdsToKill.length < numExecutorToKillWithNoShuffle &&
        !executorToBlockSize.contains(id)) {
        executorIdsToKill.append(id)
      }
    }

    // Evict the rest executors according to the shuffle block size
    executorToBlockSize.toSeq.sortBy(_._2).foreach { case (id, _) =>
      if (executorIdsToKill.length < expectedNumExecutorToKill) {
        executorIdsToKill.append(id)
      }
    }

    executorIdsToKill.toSeq
  }

  private def killExecutors(
      sc: SparkContext,
      targetExecutors: Int,
      shuffleId: Int,
      numReduce: Int): Unit = {
    val executorAllocationClient = sc.schedulerBackend.asInstanceOf[ExecutorAllocationClient]

    val executorsToKill = findExecutorToKill(sc, targetExecutors, shuffleId, numReduce)
    logInfo(s"Request to kill executors, total count ${executorsToKill.size}, " +
      s"[${executorsToKill.mkString(", ")}].")

    // Note, `SparkContext#killExecutors` does not allow with DRA enabled,
    // see `https://github.com/apache/spark/pull/20604`.
    // It may cause the status in `ExecutorAllocationManager` inconsistent with
    // `CoarseGrainedSchedulerBackend` for a while. But it should be synchronous finally.
    executorAllocationClient.killExecutors(
      executorIds = executorsToKill,
      adjustTargetNumExecutors = false,
      countFailures = false,
      force = false)
  }

  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    OptimizeSkewInRebalancePartitions,
    CoalesceShufflePartitions(session),
    OptimizeShuffleWithLocalRead)
}

trait FinalRebalanceStageHelper {
  @tailrec
  final protected def findFinalRebalanceStage(plan: SparkPlan): Option[ShuffleQueryStageExec] = {
    plan match {
      case p: ProjectExec => findFinalRebalanceStage(p.child)
      case f: FilterExec => findFinalRebalanceStage(f.child)
      case s: SortExec if !s.global => findFinalRebalanceStage(s.child)
      case stage: ShuffleQueryStageExec
          if stage.isMaterialized &&
            stage.plan.isInstanceOf[ShuffleExchangeExec] &&
            stage.plan.asInstanceOf[ShuffleExchangeExec].shuffleOrigin != ENSURE_REQUIREMENTS =>
        Some(stage)
      case _ => None
    }
  }
}
