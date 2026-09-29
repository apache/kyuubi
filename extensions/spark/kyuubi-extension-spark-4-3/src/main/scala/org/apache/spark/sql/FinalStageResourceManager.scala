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
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}

import org.apache.kyuubi.sql.{KyuubiSQLConf, WriteUtils}

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

    if (!WriteUtils.isWrite(session, plan)) {
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

    val stageOpt = findFinalRebalanceStage(plan)
    if (stageOpt.isEmpty) {
      return plan
    }

    // It's not safe to kill executors if this plan contains table cache.
    // If the executor loses then the rdd would re-compute those partition.
    if (hasTableCache(plan) &&
      conf.getConf(KyuubiSQLConf.FINAL_WRITE_STAGE_SKIP_KILLING_EXECUTORS_FOR_TABLE_CACHE)) {
      return plan
    }

    // TODO: move this to query stage optimizer when updating Spark to 3.5.x
    // Since we are in `prepareQueryStage`, the AQE shuffle read has not been applied.
    // So we need to apply it by self.
    val shuffleRead = queryStageOptimizerRules.foldLeft(stageOpt.get.asInstanceOf[SparkPlan]) {
      case (latest, rule) => rule.apply(latest)
    }
    val (targetCores, stage) = shuffleRead match {
      case AQEShuffleReadExec(stage: ShuffleQueryStageExec, partitionSpecs) =>
        (partitionSpecs.length, stage)
      case stage: ShuffleQueryStageExec =>
        // we can still kill executors if no AQE shuffle read, e.g., `.repartition(2)`
        (stage.shuffle.numPartitions, stage)
      case _ =>
        // it should never happen in current Spark, but to be safe do nothing if happens
        logWarning("BUG, Please report to Apache Kyuubi community")
        return plan
    }
    // The condition whether inject custom resource profile:
    // - target executors < active executors
    // - active executors - target executors > min executors
    val numActiveExecutors = sc.getExecutorIds().length
    val targetExecutors = (math.ceil(targetCores.toFloat / coresPerExecutor) * factor).toInt
      .max(1)
    val hasBenefits = targetExecutors < numActiveExecutors &&
      (numActiveExecutors - targetExecutors) > minExecutors
    logInfo(s"The snapshot of current executors view, " +
      s"active executors: $numActiveExecutors, min executor: $minExecutors, " +
      s"target executors: $targetExecutors, has benefits: $hasBenefits")
    if (hasBenefits) {
      val shuffleId = stage.plan.asInstanceOf[ShuffleExchangeExec].shuffleDependency.shuffleId
      val numReduce = stage.plan.asInstanceOf[ShuffleExchangeExec].numPartitions
      // Now, there is only a final rebalance stage waiting to execute and all tasks of previous
      // stage are finished. Kill redundant existed executors eagerly so the tasks of final
      // stage can be centralized scheduled.
      killExecutors(sc, targetExecutors, shuffleId, numReduce)
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
    val shuffleStatusOpt = tracker.shuffleStatuses.get(shuffleId)
    if (shuffleStatusOpt.isEmpty) {
      return Seq.empty
    }
    val shuffleStatus = shuffleStatusOpt.get
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
    executorsWithRegistrationTs.toSeq.sortBy(_._2).foreach { case (id, _) =>
      if (executorIdsToKill.length < expectedNumExecutorToKill &&
        !executorToBlockSize.contains(id)) {
        executorIdsToKill.append(id)
      }
    }

    // Evict the rest executors according to the shuffle block size
    executorToBlockSize.toSeq.sortBy(_._2).foreach { case (id, _) =>
      if (executorIdsToKill.length < expectedNumExecutorToKill && existedExecutors.contains(id)) {
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

    val executorsToKill =
      if (conf.getConf(KyuubiSQLConf.FINAL_WRITE_STAGE_EAGERLY_KILL_EXECUTORS_KILL_ALL)) {
        executorAllocationClient.getExecutorIds()
      } else {
        findExecutorToKill(sc, targetExecutors, shuffleId, numReduce)
      }
    logInfo(s"Request to kill executors, total count ${executorsToKill.size}, " +
      s"[${executorsToKill.mkString(", ")}].")
    if (executorsToKill.isEmpty) {
      return
    }

    // Note, `SparkContext#killExecutors` does not allow with DRA enabled,
    // see `https://github.com/apache/spark/pull/20604`.
    // It may cause the status in `ExecutorAllocationManager` inconsistent with
    // `CoarseGrainedSchedulerBackend` for a while. But it should be synchronous finally.
    //
    // We should adjust target num executors, otherwise `YarnAllocator` might re-request original
    // target executors if DRA has not updated target executors yet.
    // Note, DRA would re-adjust executors if there are more tasks to be executed, so we are safe.
    //
    //  * We kill executor
    //      * YarnAllocator re-request target executors
    //         * DRA can not release executors since they are new added
    // ----------------------------------------------------------------> timeline
    executorAllocationClient.killExecutors(
      executorIds = executorsToKill,
      adjustTargetNumExecutors = true,
      countFailures = false,
      force = false)

    FinalStageResourceManager.getAdjustedTargetExecutors(sc)
      .filter(_ < targetExecutors).foreach { adjustedExecutors =>
        val delta = targetExecutors - adjustedExecutors
        logInfo(s"Target executors after kill ($adjustedExecutors) is lower than required " +
          s"($targetExecutors). Requesting $delta additional executor(s).")
        executorAllocationClient.requestExecutors(delta)
      }
  }

  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    OptimizeSkewInRebalancePartitions,
    CoalesceShufflePartitions(session),
    OptimizeShuffleWithLocalRead)
}

object FinalStageResourceManager extends Logging {

  private[sql] def getAdjustedTargetExecutors(sc: SparkContext): Option[Int] = {
    sc.schedulerBackend match {
      case schedulerBackend: CoarseGrainedSchedulerBackend =>
        try {
          val field = classOf[CoarseGrainedSchedulerBackend]
            .getDeclaredField("requestedTotalExecutorsPerResourceProfile")
          field.setAccessible(true)
          schedulerBackend.synchronized {
            val requestedTotalExecutorsPerResourceProfile =
              field.get(schedulerBackend).asInstanceOf[mutable.HashMap[ResourceProfile, Int]]
            val defaultRp = sc.resourceProfileManager.defaultResourceProfile
            requestedTotalExecutorsPerResourceProfile.get(defaultRp)
          }
        } catch {
          case e: Exception =>
            logWarning("Failed to get requestedTotalExecutors of Default ResourceProfile", e)
            None
        }
      case _ => None
    }
  }
}

trait FinalRebalanceStageHelper extends AdaptiveSparkPlanHelper {
  @tailrec
  final protected def findFinalRebalanceStage(plan: SparkPlan): Option[ShuffleQueryStageExec] = {
    plan match {
      case write: DataWritingCommandExec => findFinalRebalanceStage(write.child)
      case write: V2TableWriteExec => findFinalRebalanceStage(write.child)
      case write: WriteFilesExec => findFinalRebalanceStage(write.child)
      case p: ProjectExec => findFinalRebalanceStage(p.child)
      case f: FilterExec => findFinalRebalanceStage(f.child)
      case s: SortExec if !s.global => findFinalRebalanceStage(s.child)
      case stage: ShuffleQueryStageExec
          if stage.isMaterialized && stage.mapStats.isDefined &&
            stage.plan.isInstanceOf[ShuffleExchangeExec] &&
            stage.plan.asInstanceOf[ShuffleExchangeExec].shuffleOrigin != ENSURE_REQUIREMENTS =>
        Some(stage)
      case _ => None
    }
  }

  final protected def hasTableCache(plan: SparkPlan): Boolean = {
    find(plan) {
      case _: InMemoryTableScanExec => true
      case _ => false
    }.isDefined
  }
}
