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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.internal.LogKeys
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Copy from Apache Spark `EnsureRequirements`
 *   1. remove reorder join predicates
 *   2. remove shuffle pruning
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
      case (child, distribution) =>
        // Split child's partitioning into categories
        val (other, grouped, nonGrouped) = splitKeyedPartitionings(child.outputPartitioning)

        // If non-KeyedPartitioning already satisfies, no changes needed
        if (other.exists(_.satisfies(distribution))) {
          child
        } else {
          // Check KeyedPartitioning satisfaction conditions
          val groupedSatisfies = grouped.find(_.satisfies(distribution))
          val nonGroupedSatisfiesAsIs = nonGrouped.exists(_.nonGroupedSatisfies(distribution))
          val nonGroupedSatisfiesWhenGrouped = nonGrouped.find(_.groupedSatisfies(distribution))

          // Check if any KeyedPartitioning satisfies the distribution
          if (groupedSatisfies.isDefined || nonGroupedSatisfiesAsIs
            || nonGroupedSatisfiesWhenGrouped.isDefined) {
            distribution match {
              case o: OrderedDistribution =>
                // OrderedDistribution requires grouped KeyedPartitioning with sorted keys
                // according to the distribution's ordering.
                // Find any KeyedPartitioning that satisfies via groupedSatisfies.
                val satisfyingKeyedPartitioning =
                  groupedSatisfies.orElse(nonGroupedSatisfiesWhenGrouped).get
                val attrs = satisfyingKeyedPartitioning.expressions.flatMap(_.collectLeaves())
                  .map(_.asInstanceOf[Attribute])
                val keyRowOrdering = RowOrdering.create(o.ordering, attrs)
                val keyOrdering = keyRowOrdering.on((t: InternalRowComparableWrapper) => t.row)
                if (satisfyingKeyedPartitioning.partitionKeys.sliding(2).forall {
                    case Seq(k1, k2) => keyOrdering.lteq(k1, k2)
                  }) {
                  child
                } else {
                  // Use distributePartitions to spread splits across expected partitions
                  val sortedGroupedKeys = satisfyingKeyedPartitioning.partitionKeys
                    .groupBy(identity).view.mapValues(_.size)
                    .toSeq.sortBy(_._1)(keyOrdering)
                  GroupPartitionsExec(
                    child,
                    expectedPartitionKeys = Some(sortedGroupedKeys),
                    distributePartitions = true)
                }

              case _ if groupedSatisfies.isDefined =>
                // Grouped KeyedPartitioning already satisfies
                child

              case _ if nonGroupedSatisfiesAsIs =>
                // Non-grouped KeyedPartitioning satisfies without grouping
                child

              case _ =>
                // Non-grouped KeyedPartitioning satisfies only after grouping
                GroupPartitionsExec(child)
            }
          } else {
            // No partitioning satisfies - need broadcast or shuffle
            val numPartitions = distribution.requiredNumPartitions
              .getOrElse(conf.numShufflePartitions)
            distribution match {
              case BroadcastDistribution(mode) =>
                BroadcastExchangeExec(mode, child)
              case _: StatefulOpClusteredDistribution =>
                ShuffleExchangeExec(
                  distribution.createPartitioning(numPartitions),
                  child,
                  REQUIRED_BY_STATEFUL_OPERATOR)
              case _ =>
                ShuffleExchangeExec(
                  distribution.createPartitioning(numPartitions),
                  child,
                  shuffleOrigin)
            }
          }
        }
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
      val candidateSpecs = specs.filter { case (index, spec) =>
        spec.canCreatePartitioning &&
        (!shouldConsiderMinParallelism ||
          children(index).outputPartitioning.numPartitions >= conf.defaultNumShufflePartitions)
      }
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
      //   2. All children are of the compatible key group partitioning or
      //      compatible shuffle partition id pass through partitioning
      // If both are true, skip shuffle.
      val areChildrenCompatible = parent.isDefined &&
        children.length == 2 && childrenIndexes.length == 2 && {
          val left = children.head
          val right = children(1)

          // key group compatibility check
          val newChildren = checkKeyGroupCompatible(
            parent.get,
            left,
            right,
            requiredChildDistributions)
          if (newChildren.isDefined) {
            children = newChildren.get
            true
          } else {
            // If key group check fails, check ShufflePartitionIdPassThrough compatibility
            checkShufflePartitionIdPassThroughCompatible(
              left,
              right,
              requiredChildDistributions)
          }
        }

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, _), idx)
            if areChildrenCompatible ||
              !childrenIndexes.contains(idx) =>
          child
        case ((child, dist), idx) =>
          if (bestSpecOpt.isDefined && bestSpecOpt.get.isCompatibleWith(specs(idx))) {
            bestSpecOpt match {
              // If `areChildrenCompatible` is false, we can still perform SPJ
              // by shuffling the other side based on join keys (see the else case below).
              // Hence we need to ensure that after this call, the outputPartitioning of the
              // partitioned side's BatchScanExec is grouped by join keys to match,
              // and we do that by pushing down the join keys
              case Some(KeyedShuffleSpec(_, _, Some(joinKeyPositions))) =>
                withJoinKeyPositions(child, joinKeyPositions)
              case _ => child
            }
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
              case gpe: GroupPartitionsExec => ShuffleExchangeExec(newPartitioning, gpe.child)
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
        // Before adding a SortExec, check whether a GroupPartitionsExec anywhere in the child
        // subtree can self-satisfy via sorted merge. tryEnableSortedMerge generates all alternative
        // plans where one or more GPEs have sorted merge enabled; we take the first one whose
        // outputOrdering satisfies the requirement.
        tryEnableSortedMerge(child)
          .find(newChild => SortOrder.orderingSatisfies(newChild.outputOrdering, requiredOrdering))
          .getOrElse(SortExec(requiredOrdering, global = false, child = child))
      }
    }

    children
  }

  private def hasKeyedPartitioning(p: Partitioning): Boolean = p match {
    case e: Expression => e.exists(_.isInstanceOf[KeyedPartitioning])
    case _ => false
  }

  // Generates all alternative plans in which one or more GroupPartitionsExec nodes in the subtree
  // have sorted-merge enabled (every possible combination). Returns a LazyList so the caller can
  // stop evaluating once a satisfying alternative is found.
  //
  // Pruning: traversal stops at SortExec (which reorders data, making sorted merge below it
  // pointless) and at any node whose outputPartitioning no longer carries a KeyedPartitioning.
  // This is a good heuristic, though not strictly equivalent to "ordering no longer propagates":
  // partition-key expressions are constant within each coalesced partition and therefore usually
  // prefix outputOrdering. When a node prunes the KeyedPartitioning (e.g. a Project that drops
  // partition keys), it also prunes that ordering prefix. Since Spark has no notion of constant
  // expressions in SortOrder, dropping a prefix invalidates the rest of the ordering too -- so in
  // practice the two are always pruned together.
  //
  // At each GPE the rule emits [original, sorted-merge-enabled] alternatives (or just [original]
  // when sorted merge cannot be enabled). multiTransformDownWithPruning then builds the Cartesian
  // product across all GPEs in the subtree, giving every combination.
  private[sql] def tryEnableSortedMerge(plan: SparkPlan): LazyList[SparkPlan] =
    plan.multiTransformDownWithPruning(p =>
      !p.isInstanceOf[SortExec] &&
        hasKeyedPartitioning(p.asInstanceOf[SparkPlan].outputPartitioning)) {
      case gpe: GroupPartitionsExec =>
        // Include the original so that peer GPEs are still independently considered.
        gpe +: gpe.tryEnableSortedMerge().toSeq
    }

  /**
   * Checks whether two children, `left` and `right`, of a join operator have compatible
   * `KeyedPartitioning`, and can benefit from storage-partitioned join.
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
      val specOpt = createKeyedShuffleSpec(p.outputPartitioning, cd)
      if (specOpt.isEmpty) return None
      specOpt.get
    }

    val leftSpec = specs.head
    val rightSpec = specs(1)
    val leftPartitioning = leftSpec.partitioning
    val rightPartitioning = rightSpec.partitioning

    // We don't need to alter the existing or add new `GroupPartitionsExec` when the child
    // partitionings are not modified (projected) in specs and left and right side partitionings are
    // compatible with each other.
    // Left and right `outputPartitioning` is a `PartitioningCollection` or a `KeyedPartitioning`
    // otherwise `createKeyedShuffleSpec()` would have returned `None`.
    var isCompatible =
      left.outputPartitioning.asInstanceOf[Expression].exists(_ == leftPartitioning) &&
        right.outputPartitioning.asInstanceOf[Expression].exists(_ == rightPartitioning) &&
        leftSpec.isCompatibleWith(rightSpec)
    if ((!isCompatible || conf.v2BucketingPartiallyClusteredDistributionEnabled) &&
      (conf.v2BucketingPushPartValuesEnabled ||
        conf.v2BucketingAllowJoinKeysSubsetOfPartitionKeys)) {
      logInfo("Pushing common partition values for storage-partitioned join")
      isCompatible = leftSpec.areKeysCompatible(rightSpec)

      // Partition expressions are compatible. Regardless of whether partition values
      // match from both sides of children, we can calculate a superset of partition values and
      // push-down to respective data sources so they can adjust their output partitioning by
      // filling missing partition keys with empty partitions. As result, we can still avoid
      // shuffle.
      //
      // For instance, if two sides of a join have partition expressions
      // `day(a)` and `day(b)` respectively
      // (the join query could be `SELECT ... FROM t1 JOIN t2 on t1.a = t2.b`), but
      // with different partition values:
      //   `day(a)`: [0, 1]
      //   `day(b)`: [1, 2, 3]
      // Following the case 2 above, we don't have to shuffle both sides, but instead can
      // just push the common set of partition values: `[0, 1, 2, 3]` down to the two data
      // sources.
      if (isCompatible) {
        val leftPartKeys = leftPartitioning.partitionKeys
        val rightPartKeys = rightPartitioning.partitionKeys

        val numLeftPartKeys = MDC(LogKeys.NUM_LEFT_PARTITION_VALUES, leftPartKeys.size)
        val numRightPartKeys = MDC(LogKeys.NUM_RIGHT_PARTITION_VALUES, rightPartKeys.size)
        logInfo(
          log"""
               |Left side # of partitions: $numLeftPartKeys
               |Right side # of partitions: $numRightPartKeys
               |""".stripMargin)

        // in case of compatible but not identical partition expressions, we apply 'reduce'
        // transforms to group one side's partitions as well as the common partition values
        val leftReducers = leftSpec.reducers(rightSpec)
        val rightReducers = rightSpec.reducers(leftSpec)
        val (leftReducedDataTypes, leftReducedKeys) = leftReducers.fold(
          (
            leftPartitioning.expressionDataTypes,
            leftPartitioning.partitionKeys))(leftPartitioning.reduceKeys)
        val (rightReducedDataTypes, rightReducedKeys) = rightReducers.fold(
          (
            rightPartitioning.expressionDataTypes,
            rightPartitioning.partitionKeys))(rightPartitioning.reduceKeys)
        val reducedDataTypes = if (leftReducedDataTypes == rightReducedDataTypes) {
          leftReducedDataTypes
        } else {
          // QueryExecutionErrors.storagePartitionJoinIncompatibleReducedTypesError is
          // private[sql] in Spark and thus inaccessible from kyuubi; fall back to a generic
          // internal error carrying the same diagnostic information.
          throw SparkException.internalError(
            s"Storage partition join has incompatible reduced key data types: " +
              s"left=$leftReducedDataTypes with reducers=$leftReducers, " +
              s"right=$rightReducedDataTypes with reducers=$rightReducers")
        }

        val reducedKeyRowOrdering = RowOrdering.createNaturalAscendingOrdering(reducedDataTypes)
        val reducedKeyOrdering =
          reducedKeyRowOrdering.on((t: InternalRowComparableWrapper) => t.row)

        // merge values on both sides
        var mergedPartitionKeys =
          mergeAndDedupPartitions(leftReducedKeys, rightReducedKeys, joinType, reducedKeyOrdering)
            .map((_, 1))

        logInfo(log"After merging, there are " +
          log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartitionKeys.size)} partitions")

        var replicateLeftSide = false
        var replicateRightSide = false
        var applyPartialClustering = false

        // This means we allow partitions that are not clustered on their values,
        // that is, multiple partitions with the same partition value. In the
        // following, we calculate how many partitions that each distinct partition
        // value has, and pushdown the information to scans, so they can adjust their
        // final input partitions respectively.
        if (conf.v2BucketingPartiallyClusteredDistributionEnabled) {
          logInfo("Calculating partially clustered distribution for " +
            "storage-partitioned join")

          // Similar to `OptimizeSkewedJoin`, we need to check join type and decide
          // whether partially clustered distribution can be applied. For instance, the
          // optimization cannot be applied to a left outer join, where the left hand
          // side is chosen as the side to replicate partitions according to stats.
          // Otherwise, query result could be incorrect.
          val canReplicateLeft = canReplicateLeftSide(joinType)
          val canReplicateRight = canReplicateRightSide(joinType)

          if (!canReplicateLeft && !canReplicateRight) {
            logInfo(log"Skipping partially clustered distribution as it cannot be applied for " +
              log"join type '${MDC(LogKeys.JOIN_TYPE, joinType)}'")
          } else {
            val unwrappedLeft = unwrapGroupPartitions(left)
            val unwrappedRight = unwrapGroupPartitions(right)

            val leftLink = unwrappedLeft.logicalLink
            val rightLink = unwrappedRight.logicalLink

            replicateLeftSide =
              if (leftLink.isDefined && rightLink.isDefined &&
                leftLink.get.stats.sizeInBytes > 1 &&
                rightLink.get.stats.sizeInBytes > 1) {
                val leftLinkStatsSizeInBytes =
                  MDC(LogKeys.LEFT_LOGICAL_PLAN_STATS_SIZE_IN_BYTES, leftLink.get.stats.sizeInBytes)
                val rightLinkStatsSizeInBytes = MDC(
                  LogKeys.RIGHT_LOGICAL_PLAN_STATS_SIZE_IN_BYTES,
                  rightLink.get.stats.sizeInBytes)
                logInfo(
                  log"""
                       |Using plan statistics to determine which side of join to fully
                       |cluster partition values:
                       |Left side size (in bytes): $leftLinkStatsSizeInBytes
                       |Right side size (in bytes): $rightLinkStatsSizeInBytes
                       |""".stripMargin)
                leftLink.get.stats.sizeInBytes < rightLink.get.stats.sizeInBytes
              } else {
                // As a simple heuristic, we pick the side with fewer number of partitions
                // to apply the grouping & replication of partitions
                logInfo("Using number of partitions to determine which side of join " +
                  "to fully cluster partition values")
                leftPartKeys.size < rightPartKeys.size
              }

            replicateRightSide = !replicateLeftSide

            // Similar to skewed join, we need to check the join type to see whether replication
            // of partitions can be applied. For instance, replication should not be allowed for
            // the left-hand side of a right outer join.
            if (replicateLeftSide && !canReplicateLeft) {
              logInfo(log"Left-hand side is picked but cannot be applied to join type " +
                log"'${MDC(LogKeys.JOIN_TYPE, joinType)}'. Skipping partially clustered " +
                log"distribution.")
              replicateLeftSide = false
            } else if (replicateRightSide && !canReplicateRight) {
              logInfo(log"Right-hand side is picked but cannot be applied to join type " +
                log"'${MDC(LogKeys.JOIN_TYPE, joinType)}'. Skipping partially clustered " +
                log"distribution.")
              replicateRightSide = false
            } else {
              // In partially clustered distribution, we should use un-grouped partition values
              val (partiallyClusteredChild, partiallyClusteredSpec) = if (replicateLeftSide) {
                (unwrappedRight, rightSpec)
              } else {
                (unwrappedLeft, leftSpec)
              }
              // Original `KeyedPartitioning` can be obtained from the child directly if the child
              // satisfied the distribution requirement; or from the child's child if it didn't as
              // the child must be a `GroupPartitionsExec` inserted by `EnsureRequirement`
              // to satisfy the distribution requirement.
              val originalPartitioning =
                partiallyClusteredChild.outputPartitioning.asInstanceOf[Expression]
              // `outputPartitioning` is either a `PartitioningCollection` or a `KeyedPartitioning`
              // otherwise `createKeyedShuffleSpec()` would have returned `None`.
              val originalKeyedPartitioning =
                originalPartitioning.collectFirst { case k: KeyedPartitioning => k }.get
              val projectedOriginalPartitionKeys = partiallyClusteredSpec.joinKeyPositions
                .fold(originalKeyedPartitioning.partitionKeys)(
                  originalKeyedPartitioning.projectKeys(_)._2)

              val numExpectedPartitions =
                projectedOriginalPartitionKeys.groupBy(identity).view.mapValues(_.size)

              mergedPartitionKeys = mergedPartitionKeys.map { case (key, numParts) =>
                (key, numExpectedPartitions.getOrElse(key, numParts))
              }

              logInfo(log"After applying partially clustered distribution, there are " +
                log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartitionKeys.map(_._2).sum)} partitions.")
              applyPartialClustering = true
            }
          }
        }

        // Now we need to push-down the common partition information to the `GroupPartitionsExec`s.
        newLeft = applyGroupPartitions(
          left,
          leftSpec.joinKeyPositions,
          mergedPartitionKeys,
          leftReducers,
          distributePartitions = applyPartialClustering && !replicateLeftSide)
        newRight = applyGroupPartitions(
          right,
          rightSpec.joinKeyPositions,
          mergedPartitionKeys,
          rightReducers,
          distributePartitions = applyPartialClustering && !replicateRightSide)
      }
    }

    if (isCompatible) Some(Seq(newLeft, newRight)) else None
  }

  private def checkShufflePartitionIdPassThroughCompatible(
      left: SparkPlan,
      right: SparkPlan,
      requiredChildDistribution: Seq[Distribution]): Boolean = {
    (left.outputPartitioning, right.outputPartitioning) match {
      case (p1: ShufflePartitionIdPassThrough, p2: ShufflePartitionIdPassThrough) =>
        assert(requiredChildDistribution.length == 2)
        val leftSpec = p1.createShuffleSpec(
          requiredChildDistribution.head.asInstanceOf[ClusteredDistribution])
        val rightSpec = p2.createShuffleSpec(
          requiredChildDistribution(1).asInstanceOf[ClusteredDistribution])
        leftSpec.isCompatibleWith(rightSpec)
      case _ =>
        false
    }
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

  /**
   * Unwraps a GroupPartitionsExec to get the underlying child plan.
   */
  private def unwrapGroupPartitions(plan: SparkPlan): SparkPlan = plan match {
    case g: GroupPartitionsExec => g.child
    case other => other
  }

  /**
   * Applies or updates `GroupPartitionsExec` with the given parameters.
   *
   * `GroupPartitionsExec` can be either the given plan node (child of the join inserted by
   * `EnsureRequirement`) if the original child didn't satisfy the distribution requirement; or we
   * can create a new one specifically for this join.
   */
  private def applyGroupPartitions(
      plan: SparkPlan,
      joinKeyPositions: Option[Seq[Int]],
      mergedPartitionKeys: Seq[(InternalRowComparableWrapper, Int)],
      reducers: Option[Seq[Option[Reducer[_, _]]]],
      distributePartitions: Boolean): SparkPlan = {
    plan match {
      case g: GroupPartitionsExec =>
        val newGroupPartitions = g.copy(
          joinKeyPositions = joinKeyPositions,
          expectedPartitionKeys = Some(mergedPartitionKeys),
          reducers = reducers,
          distributePartitions = distributePartitions)
        newGroupPartitions.copyTagsFrom(g)
        newGroupPartitions
      case _ =>
        GroupPartitionsExec(
          plan,
          joinKeyPositions,
          Some(mergedPartitionKeys),
          reducers,
          distributePartitions)
    }
  }

  /**
   * Applies join key positions to a plan by wrapping or updating GroupPartitionsExec.
   */
  private def withJoinKeyPositions(plan: SparkPlan, positions: Seq[Int]): SparkPlan = {
    plan match {
      case g: GroupPartitionsExec =>
        val newGroupPartitions = g.copy(joinKeyPositions = Some(positions))
        newGroupPartitions.copyTagsFrom(g)
        newGroupPartitions
      case _ => GroupPartitionsExec(plan, joinKeyPositions = Some(positions))
    }
  }

  /**
   * Tries to create a [[KeyedShuffleSpec]] from the input partitioning and distribution, if the
   * partitioning is a [[KeyedPartitioning]] (either directly or indirectly), and satisfies the
   * given distribution.
   */
  private def createKeyedShuffleSpec(
      partitioning: Partitioning,
      distribution: ClusteredDistribution): Option[KeyedShuffleSpec] = {
    def tryCreate(partitioning: KeyedPartitioning): Option[KeyedShuffleSpec] = {
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
        Some(partitioning.createShuffleSpec(distribution).asInstanceOf[KeyedShuffleSpec])
      } else {
        None
      }
    }

    partitioning match {
      case p: KeyedPartitioning => tryCreate(p)
      case PartitioningCollection(partitionings) =>
        partitionings.collectFirst(Function.unlift(createKeyedShuffleSpec(_, distribution)))
      case _ => None
    }
  }

  /**
   * Merge, dedup and sort partitions keys for SPJ and optionally enable partition filtering.
   * Both sides must have matching partition expressions.
   * @param leftPartitionKeys left side partition keys
   * @param rightPartitionKeys right side partition keys
   * @param joinType join type for optional partition filtering
   * @param keyOrdering ordering to sort partition keys
   * @return merged and sorted partition values
   */
  def mergeAndDedupPartitions(
      leftPartitionKeys: Seq[InternalRowComparableWrapper],
      rightPartitionKeys: Seq[InternalRowComparableWrapper],
      joinType: JoinType,
      keyOrdering: Ordering[InternalRowComparableWrapper]): Seq[InternalRowComparableWrapper] = {
    val merged = if (SQLConf.get.getConf(SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED)) {
      joinType match {
        case Inner =>
          mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys, intersect = true)
        case LeftOuter => leftPartitionKeys.distinct
        case RightOuter => rightPartitionKeys.distinct
        case _ => mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys)
      }
    } else {
      mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys)
    }

    // SPARK-41471: We keep to order of partitions to make sure the order of
    // partitions is deterministic in different case.
    merged.sorted(keyOrdering)
  }

  private def mergeAndDedupPartitionKeys(
      leftPartitionKeys: Seq[InternalRowComparableWrapper],
      rightPartitionKeys: Seq[InternalRowComparableWrapper],
      intersect: Boolean = false) = {
    val leftKeySet = mutable.HashSet.from(leftPartitionKeys)
    val rightKeySet = mutable.HashSet.from(rightPartitionKeys)
    val result = if (intersect) {
      leftKeySet.intersect(rightKeySet)
    } else {
      leftKeySet.union(rightKeySet)
    }
    result.toSeq
  }

  /**
   * Splits a partitioning into three categories:
   * 1. Non-KeyedPartitioning (HashPartitioning, RangePartitioning, etc.)
   * 2. Grouped KeyedPartitioning (isGrouped = true)
   * 3. Non-grouped KeyedPartitioning (isGrouped = false)
   *
   * @param partitioning The partitioning to split
   * @return A tuple of (other, grouped, nonGrouped) where:
   *         - other: Option containing non-KeyedPartitioning(s)
   *         - grouped: Seq of grouped KeyedPartitionings
   *         - nonGrouped: Seq of non-grouped KeyedPartitionings
   */
  private def splitKeyedPartitionings(partitioning: Partitioning) = {
    val otherPartitionings = ArrayBuffer.empty[Partitioning]
    val groupedKeyedPartitionings = ArrayBuffer.empty[KeyedPartitioning]
    val nonGroupedKeyedPartitionings = ArrayBuffer.empty[KeyedPartitioning]

    def split(p: Partitioning): Unit = p match {
      case c: PartitioningCollection => c.partitionings.foreach(split)
      case k: KeyedPartitioning =>
        if (k.isGrouped) {
          groupedKeyedPartitionings += k
        } else {
          nonGroupedKeyedPartitionings += k
        }
      case o => otherPartitionings += o
    }

    split(partitioning)

    val other = otherPartitionings.length match {
      case 0 => None
      case 1 => Some(otherPartitionings.head)
      case _ => Some(PartitioningCollection(otherPartitionings.toSeq))
    }

    (other, groupedKeyedPartitionings.toSeq, nonGroupedKeyedPartitionings.toSeq)
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
