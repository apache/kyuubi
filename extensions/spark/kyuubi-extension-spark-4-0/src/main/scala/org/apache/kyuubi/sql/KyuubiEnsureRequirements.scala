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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Copy from Apache Spark `EnsureRequirements`
 *   1. remove reorder join predicates
 *   2. remove shuffle pruning
 *
 * TODO: bring back the v2 bucketing join handling logic
 */
object KyuubiEnsureRequirements extends Rule[SparkPlan] {

  private def ensureDistributionAndOrdering(
      parent: Option[SparkPlan],
      originalChildren: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution],
      requiredChildOrderings: Seq[Seq[SortOrder]],
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = {
    assert(requiredChildDistributions.length == originalChildren.length)
    assert(requiredChildOrderings.length == originalChildren.length)
    // Ensure that the operator's children satisfy their output distribution requirements.
    var children = originalChildren.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
          .getOrElse(conf.numShufflePartitions)
        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child, shuffleOrigin)
    }

    // Get the indexes of children which have specified distribution requirements and need to be
    // co-partitioned.
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (_: ClusteredDistribution, _) => true
      case _ => false
    }.map(_._2)

    // Special case: if all sides of the join are single partition and it's physical size less than
    // or equal spark.sql.maxSinglePartitionBytes.
    val preferSinglePartition = childrenIndexes.forall { i =>
      children(i).outputPartitioning == SinglePartition &&
      children(i).logicalLink
        .forall(_.stats.sizeInBytes <= conf.getConf(SQLConf.MAX_SINGLE_PARTITION_BYTES))
    }

    // If there are more than one children, we'll need to check partitioning & distribution of them
    // and see if extra shuffles are necessary.
    if (childrenIndexes.length > 1 && !preferSinglePartition) {
      val specs = childrenIndexes.map(i => {
        val requiredDist = requiredChildDistributions(i)
        assert(
          requiredDist.isInstanceOf[ClusteredDistribution],
          s"Expected ClusteredDistribution but found ${requiredDist.getClass.getSimpleName}")
        i -> children(i).outputPartitioning.createShuffleSpec(
          requiredDist.asInstanceOf[ClusteredDistribution])
      }).toMap

      // Find out the shuffle spec that gives better parallelism. Currently this is done by
      // picking the spec with the largest number of partitions.
      //
      // NOTE: this is not optimal for the case when there are more than 2 children. Consider:
      //   (10, 10, 11)
      // where the number represent the number of partitions for each child, it's better to pick 10
      // here since we only need to shuffle one side - we'd need to shuffle two sides if we pick 11.
      //
      // However this should be sufficient for now since in Spark nodes with multiple children
      // always have exactly 2 children.

      // Whether we should consider `spark.sql.shuffle.partitions` and ensure enough parallelism
      // during shuffle. To achieve a good trade-off between parallelism and shuffle cost, we only
      // consider the minimum parallelism iff ALL children need to be re-shuffled.
      //
      // A child needs to be re-shuffled iff either one of below is true:
      //   1. It can't create partitioning by itself, i.e., `canCreatePartitioning` returns false
      //      (as for the case of `RangePartitioning`), therefore it needs to be re-shuffled
      //      according to other shuffle spec.
      //   2. It already has `ShuffleExchangeLike`, so we can re-use existing shuffle without
      //      introducing extra shuffle.
      //
      // On the other hand, in scenarios such as:
      //   HashPartitioning(5) <-> HashPartitioning(6)
      // while `spark.sql.shuffle.partitions` is 10, we'll only re-shuffle the left side and make it
      // HashPartitioning(6).
      val shouldConsiderMinParallelism = specs.forall(p =>
        !p._2.canCreatePartitioning || children(p._1).isInstanceOf[ShuffleExchangeLike])
      // Choose all the specs that can be used to shuffle other children
      val candidateSpecs = specs
        .filter(_._2.canCreatePartitioning)
        .filter(p =>
          !shouldConsiderMinParallelism ||
            children(p._1).outputPartitioning.numPartitions >= conf.defaultNumShufflePartitions)
      val bestSpecOpt = if (candidateSpecs.isEmpty) {
        None
      } else {
        // When choosing specs, we should consider those children with no `ShuffleExchangeLike` node
        // first. For instance, if we have:
        //   A: (No_Exchange, 100) <---> B: (Exchange, 120)
        // it's better to pick A and change B to (Exchange, 100) instead of picking B and insert a
        // new shuffle for A.
        val candidateSpecsWithoutShuffle = candidateSpecs.filter { case (k, _) =>
          !children(k).isInstanceOf[ShuffleExchangeLike]
        }
        val finalCandidateSpecs = if (candidateSpecsWithoutShuffle.nonEmpty) {
          candidateSpecsWithoutShuffle
        } else {
          candidateSpecs
        }
        // Pick the spec with the best parallelism
        Some(finalCandidateSpecs.values.maxBy(_.numPartitions))
      }

      // Check if the following conditions are satisfied:
      //   1. There are exactly two children (e.g., join). Note that Spark doesn't support
      //      multi-way join at the moment, so this check should be sufficient.
      //   2. All children are of `KeyGroupedPartitioning`, and they are compatible with each other
      // If both are true, skip shuffle.
      val isKeyGroupCompatible = parent.isDefined &&
        children.length == 2 && childrenIndexes.length == 2 && {
          val left = children.head
          val right = children(1)
          val newChildren = checkKeyGroupCompatible(
            parent.get,
            left,
            right,
            requiredChildDistributions)
          if (newChildren.isDefined) {
            children = newChildren.get
          }
          newChildren.isDefined
        }

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, _), idx) if isKeyGroupCompatible || !childrenIndexes.contains(idx) =>
          child
        case ((child, dist), idx) =>
          if (bestSpecOpt.isDefined && bestSpecOpt.get.isCompatibleWith(specs(idx))) {
            child
          } else {
            val newPartitioning = bestSpecOpt.map { bestSpec =>
              // Use the best spec to create a new partitioning to re-shuffle this child
              val clustering = dist.asInstanceOf[ClusteredDistribution].clustering
              bestSpec.createPartitioning(clustering)
            }.getOrElse {
              // No best spec available, so we create default partitioning from the required
              // distribution
              val numPartitions = dist.requiredNumPartitions
                .getOrElse(conf.numShufflePartitions)
              dist.createPartitioning(numPartitions)
            }

            child match {
              case ShuffleExchangeExec(_, c, so, ps) =>
                ShuffleExchangeExec(newPartitioning, c, so, ps)
              case _ => ShuffleExchangeExec(newPartitioning, child)
            }
          }
      }
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        SortExec(requiredOrdering, global = false, child = child)
      }
    }

    children
  }

  /**
   * Checks whether two children, `left` and `right`, of a join operator have compatible
   * `KeyGroupedPartitioning`, and can benefit from storage-partitioned join.
   *
   * Returns the updated new children if the check is successful, otherwise `None`.
   */
  private def checkKeyGroupCompatible(
      parent: SparkPlan,
      left: SparkPlan,
      right: SparkPlan,
      requiredChildDistribution: Seq[Distribution]): Option[Seq[SparkPlan]] = {
    parent match {
      case smj: SortMergeJoinExec =>
        checkKeyGroupCompatible(left, right, smj.joinType, requiredChildDistribution)
      case sj: ShuffledHashJoinExec =>
        checkKeyGroupCompatible(left, right, sj.joinType, requiredChildDistribution)
      case _ =>
        None
    }
  }

  private def checkKeyGroupCompatible(
      left: SparkPlan,
      right: SparkPlan,
      joinType: JoinType,
      requiredChildDistribution: Seq[Distribution]): Option[Seq[SparkPlan]] = {
    assert(requiredChildDistribution.length == 2)

    var newLeft = left
    var newRight = right

    val specs = Seq(left, right).zip(requiredChildDistribution).map { case (p, d) =>
      if (!d.isInstanceOf[ClusteredDistribution]) return None
      val cd = d.asInstanceOf[ClusteredDistribution]
      val specOpt = createKeyGroupedShuffleSpec(p.outputPartitioning, cd)
      if (specOpt.isEmpty) return None
      specOpt.get
    }

    val leftSpec = specs.head
    val rightSpec = specs(1)

    val isCompatible = leftSpec.isCompatibleWith(rightSpec)

    if (isCompatible) Some(Seq(newLeft, newRight)) else None
  }

  // Similar to `OptimizeSkewedJoin.canSplitRightSide`
  private def canReplicateLeftSide(joinType: JoinType): Boolean = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  // Similar to `OptimizeSkewedJoin.canSplitLeftSide`
  private def canReplicateRightSide(joinType: JoinType): Boolean = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
    joinType == LeftAnti || joinType == LeftOuter
  }

  // Populate the common partition values down to the scan nodes
  private def populatePartitionValues(
      plan: SparkPlan,
      values: Seq[(InternalRow, Int)],
      applyPartialClustering: Boolean,
      replicatePartitions: Boolean): SparkPlan = plan match {
    case scan: BatchScanExec =>
      scan.copy(spjParams = scan.spjParams.copy(
        commonPartitionValues = Some(values),
        applyPartialClustering = applyPartialClustering,
        replicatePartitions = replicatePartitions))
    case node =>
      node.mapChildren(child =>
        populatePartitionValues(
          child,
          values,
          applyPartialClustering,
          replicatePartitions))
  }

  /**
   * Tries to create a [[KeyGroupedShuffleSpec]] from the input partitioning and distribution, if
   * the partitioning is a [[KeyGroupedPartitioning]] (either directly or indirectly), and
   * satisfies the given distribution.
   */
  private def createKeyGroupedShuffleSpec(
      partitioning: Partitioning,
      distribution: ClusteredDistribution): Option[KeyGroupedShuffleSpec] = {
    def check(partitioning: KeyGroupedPartitioning): Option[KeyGroupedShuffleSpec] = {
      val attributes = partitioning.expressions.flatMap(_.collectLeaves())
      val clustering = distribution.clustering

      val satisfies = if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
        attributes.length == clustering.length && attributes.zip(clustering).forall {
          case (l, r) => l.semanticEquals(r)
        }
      } else {
        partitioning.satisfies(distribution)
      }

      if (satisfies) {
        Some(partitioning.createShuffleSpec(distribution).asInstanceOf[KeyGroupedShuffleSpec])
      } else {
        None
      }
    }

    partitioning match {
      case p: KeyGroupedPartitioning => check(p)
      case PartitioningCollection(partitionings) =>
        val specs = partitionings.map(p => createKeyGroupedShuffleSpec(p, distribution))
        assert(specs.forall(_.isEmpty) || specs.forall(_.isDefined))
        specs.head
      case _ => None
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan =>
      val newChildren = ensureDistributionAndOrdering(
        Some(operator),
        operator.children,
        operator.requiredChildDistribution,
        operator.requiredChildOrdering,
        ENSURE_REQUIREMENTS)
      operator.withNewChildren(newChildren)
  }
}
