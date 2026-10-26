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

package org.apache.spark.sql.execution.exchange

import scala.collection.immutable.BitSet
import scala.collection.mutable

import org.apache.spark.internal.LogKeys
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Copy from Apache Spark `EnsureRequirements`
 *   1. remove reorder join predicates
 *   2. remove shuffle pruning
 */
object KyuubiEnsureRequirements extends Rule[SparkPlan] {

  private type KeyedResolution =
    Either[KeyedPartitioning, (KeyedPartitioning, Option[Seq[Int]])]

  private def ensureDistributionAndOrdering(
      parent: Option[SparkPlan],
      originalChildren: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution],
      requiredChildOrderings: Seq[Seq[SortOrder]],
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = {
    assert(requiredChildDistributions.length == originalChildren.length)
    assert(requiredChildOrderings.length == originalChildren.length)
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (_: ClusteredDistribution, _) => true
      case _ => false
    }.map(_._2)
    val isCoPartitioned = childrenIndexes.length > 1
    // Ensure that the operator's children satisfy their output distribution requirements.
    var children = originalChildren.zip(requiredChildDistributions).map {
      case (child, distribution) =>
        val (otherSatisfies, keyed) =
          splitKeyedPartitionings(child.outputPartitioning, distribution, isCoPartitioned)

        // If non-KeyedPartitioning already satisfies, no changes needed
        if (otherSatisfies) {
          child
        } else {
          keyed match {
            case Some(resolution) =>
              (distribution, resolution) match {
                case (ordering: OrderedDistribution, _) =>
                  // OrderedDistribution requires grouped KeyedPartitioning with sorted keys
                  // according to the distribution's ordering.
                  val satisfyingKeyedPartitioning = resolution.fold(identity, _._1)
                  assert(
                    ordering.areAllClusterKeysMatched(satisfyingKeyedPartitioning.expressions),
                    "the partition expressions must be the ordering's, position by position")
                  val keyRowOrdering = RowOrdering.create(
                    ordering.ordering.zip(satisfyingKeyedPartitioning.keyDataTypes)
                      .zipWithIndex.map {
                        case ((sortOrder, dataType), index) =>
                          sortOrder.copy(
                            child = BoundReference(index, dataType, nullable = true),
                            sameOrderExpressions = Seq.empty)
                      },
                    Nil)
                  val keyOrdering =
                    keyRowOrdering.on((key: InternalRowComparableWrapper) => key.row)
                  val keys = satisfyingKeyedPartitioning.partitionKeys
                  if (keys.zip(keys.drop(1)).forall {
                      case (leftKey, rightKey) => keyOrdering.lteq(leftKey, rightKey)
                    }) {
                    child
                  } else {
                    // Use distributePartitions to spread splits across expected partitions
                    val sortedGroupedKeys = keys
                      .groupBy(identity).view.mapValues(_.size)
                      .toSeq.sortBy(_._1)(keyOrdering)
                    GroupPartitionsExec(
                      child,
                      expectedPartitionKeys = Some(sortedGroupedKeys),
                      distributePartitions = true)
                  }

                case (_, scala.Left(_)) =>
                  child

                case (_, scala.Right((_, positions))) =>
                  GroupPartitionsExec(child, joinKeyPositions = positions)
              }
            case None =>
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
      def servingMembers(spec: ShuffleSpec): Seq[ShuffleSpec] =
        flattenSpec(spec).filter(_.canCreatePartitioning)
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
        Some(finalCandidateSpecs.values.maxBy(servingMembers(_).head.numPartitions))
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

      lazy val matchedIndexes = bestSpecOpt.toSeq.flatMap { best =>
        childrenIndexes.filter(index =>
          servingMembers(best).exists(_.isCompatibleWith(specs(index))))
      }
      lazy val bestMemberOpt = bestSpecOpt.flatMap { best =>
        val matchedMembers = matchedIndexes.map(index => flattenSpec(specs(index)))
        servingMembers(best)
          .filter(member => matchedMembers.forall(_.exists(member.isCompatibleWith)))
          .maxByOption(_.numPartitions)
      }

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, _), idx)
            if areChildrenCompatible ||
              !childrenIndexes.contains(idx) =>
          child
        case ((child, dist), idx) =>
          if (bestMemberOpt.isDefined && matchedIndexes.contains(idx)) {
            val bestMember = bestMemberOpt.get
            flattenSpec(specs(idx)).find(bestMember.isCompatibleWith) match {
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
            val newPartitioning = bestMemberOpt.map { bestMember =>
              // Use the best spec to create a new partitioning to re-shuffle this child
              val clustering = dist.asInstanceOf[ClusteredDistribution].clustering
              bestMember.createPartitioning(clustering)
            }.getOrElse {
              // No best spec available, so we create default partitioning from the required
              // distribution
              val numPartitions = dist.requiredNumPartitions
                .getOrElse(conf.numShufflePartitions)
              dist.createPartitioning(numPartitions)
            }

            child match {
              case exchange: ShuffleExchangeExec =>
                exchange.copy(outputPartitioning = newPartitioning)
              case grouping: GroupPartitionsExec =>
                ShuffleExchangeExec(newPartitioning, unwrapGroupPartitions(grouping))
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
        conf.v2BucketingAllowKeysSubsetOfPartitionKeys)) {
      logInfo("Pushing common partition values for storage-partitioned join")
      isCompatible = leftSpec.areKeysCompatible(rightSpec, allowReduce = true)

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
        val (leftReducers, rightReducers) = leftSpec.reducersBothWays(rightSpec)
        val (leftReducedDataTypes, leftReducedKeys) = leftReducers.fold(
          (
            leftPartitioning.keyDataTypes,
            leftPartitioning.partitionKeys))(leftPartitioning.reduceKeys)
        val (rightReducedDataTypes, rightReducedKeys) = rightReducers.fold(
          (
            rightPartitioning.keyDataTypes,
            rightPartitioning.partitionKeys))(rightPartitioning.reduceKeys)
        val leftTypesDescribeKeys =
          leftReducedKeys.nonEmpty || leftPartitioning.expressionsDescribeKeys
        val rightTypesDescribeKeys =
          rightReducedKeys.nonEmpty || rightPartitioning.expressionsDescribeKeys
        val reducedDataTypes = if (!leftTypesDescribeKeys) {
          rightReducedDataTypes
        } else if (!rightTypesDescribeKeys || leftReducedDataTypes == rightReducedDataTypes) {
          leftReducedDataTypes
        } else {
          throw QueryExecutionErrors.storagePartitionJoinIncompatibleReducedTypesError(
            leftReducers = leftReducers,
            leftReducedDataTypes = leftReducedDataTypes,
            rightReducers = rightReducers,
            rightReducedDataTypes = rightReducedDataTypes)
        }

        val reducedKeyRowOrdering = KeyedPartitioning.groupedKeyRowOrdering(reducedDataTypes)
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
            val leftGrouping = innermostGroupPartition(left)
            val rightGrouping = innermostGroupPartition(right)
            val unwrappedLeft = leftGrouping.map(_._1.child).getOrElse(left)
            val unwrappedRight = rightGrouping.map(_._1.child).getOrElse(right)

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
                PartitioningCollection.numKeyedPartitions(unwrappedLeft.outputPartitioning)
                  .getOrElse(leftPartKeys.size) <
                  PartitioningCollection.numKeyedPartitions(unwrappedRight.outputPartitioning)
                    .getOrElse(rightPartKeys.size)
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
              val (partiallyClusteredChild, partiallyClusteredPositions) = if (replicateLeftSide) {
                (
                  unwrappedRight,
                  rightGrouping.flatMap(_._1.joinKeyPositions)
                    .orElse(rightSpec.joinKeyPositions))
              } else {
                (
                  unwrappedLeft,
                  leftGrouping.flatMap(_._1.joinKeyPositions)
                    .orElse(leftSpec.joinKeyPositions))
              }
              val originalPartitioning =
                partiallyClusteredChild.outputPartitioning.asInstanceOf[Expression]
              // `outputPartitioning` is either a `PartitioningCollection` or a `KeyedPartitioning`
              // otherwise `createKeyedShuffleSpec()` would have returned `None`.
              val originalKeyedPartitioning =
                originalPartitioning.collectFirst { case k: KeyedPartitioning => k }.get
              val projectedOriginalPartitionKeys = partiallyClusteredPositions
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

  private def innermostGroupPartition(
      plan: SparkPlan): Option[(GroupPartitionsExec, SparkPlan => SparkPlan)] = plan match {
    case grouping: GroupPartitionsExec =>
      innermostGroupPartition(grouping.child)
        .orElse(Some((grouping, identity[SparkPlan])))
    case sort: SortExec if !sort.global =>
      innermostGroupPartition(sort.child).map { case (grouping, rebuild) =>
        (grouping, (newChild: SparkPlan) => sort.withNewChildren(Seq(rebuild(newChild))))
      }
    case _ => None
  }

  private def rewriteGroupPartitions(plan: SparkPlan)(
      rewrite: GroupPartitionsExec => GroupPartitionsExec): Option[SparkPlan] =
    innermostGroupPartition(plan).map { case (grouping, rebuild) =>
      val rewritten = rewrite(grouping)
      rewritten.copyTagsFrom(grouping)
      rebuild(rewritten)
    }

  private def unwrapGroupPartitions(plan: SparkPlan): SparkPlan =
    innermostGroupPartition(plan).map(_._1.child).getOrElse(plan)

  private def applyGroupPartitions(
      plan: SparkPlan,
      joinKeyPositions: Option[Seq[Int]],
      mergedPartitionKeys: Seq[(InternalRowComparableWrapper, Int)],
      reducers: Option[Seq[Option[KeyReducer]]],
      distributePartitions: Boolean): SparkPlan = {
    rewriteGroupPartitions(plan) { grouping =>
      grouping.copy(
        joinKeyPositions = grouping.joinKeyPositions.orElse(joinKeyPositions),
        expectedPartitionKeys = Some(mergedPartitionKeys),
        reducers = reducers,
        distributePartitions = distributePartitions)
    }.getOrElse {
      GroupPartitionsExec(
        plan,
        joinKeyPositions,
        Some(mergedPartitionKeys),
        reducers,
        distributePartitions)
    }
  }

  private def flattenSpec(spec: ShuffleSpec): Seq[ShuffleSpec] = spec match {
    case ShuffleSpecCollection(specs) => specs.flatMap(flattenSpec)
    case other => Seq(other)
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
      val attributes = partitioning.expressions.flatMap(_.references)
      val clustering = distribution.clustering

      val satisfies = if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
        partitioning.expressions.forall(_.references.size == 1) &&
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

  private def clusterKeyPositions(
      partitioning: KeyedPartitioning,
      distribution: Distribution,
      isCoPartitioned: Boolean): BitSet = distribution match {
    case clustered: ClusteredDistribution if !isCoPartitioned =>
      partitioning.expressions.zipWithIndex.collect {
        case (expression, index)
            if clustered.clustering.exists(_.semanticEquals(expression)) ||
              expression.references.exists(reference =>
                clustered.clustering.exists(_.semanticEquals(reference))) => index
      }.to(BitSet)
    case _ => partitioning.expressions.indices.to(BitSet)
  }

  private def splitKeyedPartitionings(
      partitioning: Partitioning,
      distribution: Distribution,
      isCoPartitioned: Boolean): (Boolean, Option[KeyedResolution]) = {
    val flattened = PartitioningCollection.flatten(partitioning)
    if (flattened.exists(partitioning =>
        !partitioning.isInstanceOf[KeyedPartitioning] && partitioning.satisfies(distribution))) {
      (true, None)
    } else {
      val keyed = flattened.collect { case partitioning: KeyedPartitioning => partitioning }
      (false, resolveKeyedPartitioning(keyed, distribution, isCoPartitioned))
    }
  }

  private def resolveKeyedPartitioning(
      keyedPartitionings: Seq[KeyedPartitioning],
      distribution: Distribution,
      isCoPartitioned: Boolean): Option[KeyedResolution] = {
    val projectedNumPartitions = mutable.Map.empty[BitSet, Int]
    def numPartitionsAfter(partitioning: KeyedPartitioning, positions: BitSet): Int =
      projectedNumPartitions.getOrElseUpdate(
        positions,
        partitioning.numPartitionsProjectedOn(positions.toSeq))

    val admitted = keyedPartitionings.flatMap { partitioning =>
      val satisfies = partitioning.satisfies(distribution)
      if (satisfies || partitioning.keysMaySatisfy(distribution)) {
        val positions = clusterKeyPositions(partitioning, distribution, isCoPartitioned)
        Option.when(positions.nonEmpty || partitioning.expressions.isEmpty)(
          (partitioning, satisfies, positions))
      } else {
        None
      }
    }

    val satisfiedAsIs = admitted
      .find { case (partitioning, satisfies, positions) =>
        satisfies && positions.size == partitioning.expressions.length
      }
      .orElse(admitted.find { case (partitioning, satisfies, positions) =>
        satisfies && numPartitionsAfter(partitioning, positions) == partitioning.numPartitions
      })
      .map(_._1)

    if (satisfiedAsIs.isDefined) {
      satisfiedAsIs.map(scala.Left(_))
    } else {
      val candidates = admitted.map { case (partitioning, _, positions) =>
        partitioning -> positions
      }.distinctBy(_._2)

      val eligible = distribution.requiredNumPartitions match {
        case Some(requiredCount) => candidates.filter { case (partitioning, positions) =>
            partitioning.numPartitions >= requiredCount &&
            numPartitionsAfter(partitioning, positions) == requiredCount
          }
        case None => candidates
      }
      if (eligible.isEmpty) {
        None
      } else {
        val maximal = eligible.filter { case (_, positions) =>
          !eligible.exists { case (_, otherPositions) =>
            otherPositions.size > positions.size && positions.subsetOf(otherPositions)
          }
        }
        val (partitioning, positions) =
          if (maximal.size == 1) maximal.head
          else maximal.maxBy { case (partitioning, positions) =>
            numPartitionsAfter(partitioning, positions)
          }
        Some(scala.Right(
          (
            partitioning,
            Option.when(positions.size < partitioning.expressions.length)(positions.toSeq))))
      }
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
