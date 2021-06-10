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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}

/**
 * Copy from Apache Spark `EnsureRequirements`
 *   1. remove reorder join predicates
 *   2. remove shuffle pruning
 */
object KyuubiEnsureRequirements extends Rule[SparkPlan] {
  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements.
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
          .getOrElse(conf.numShufflePartitions)
        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child)
    }

    // Get the indexes of children which have specified distribution requirements and need to have
    // same number of partitions.
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (UnspecifiedDistribution, _) => false
      case (_: BroadcastDistribution, _) => false
      case _ => true
    }.map(_._2)

    val childrenNumPartitions =
      childrenIndexes.map(children(_).outputPartitioning.numPartitions).toSet

    if (childrenNumPartitions.size > 1) {
      // Get the number of partitions which is explicitly required by the distributions.
      val requiredNumPartitions = {
        val numPartitionsSet = childrenIndexes.flatMap {
          index => requiredChildDistributions(index).requiredNumPartitions
        }.toSet
        assert(numPartitionsSet.size <= 1,
          s"$operator have incompatible requirements of the number of partitions for its children")
        numPartitionsSet.headOption
      }

      // If there are non-shuffle children that satisfy the required distribution, we have
      // some tradeoffs when picking the expected number of shuffle partitions:
      // 1. We should avoid shuffling these children.
      // 2. We should have a reasonable parallelism.
      val nonShuffleChildrenNumPartitions =
      childrenIndexes.map(children).filterNot(_.isInstanceOf[ShuffleExchangeExec])
        .map(_.outputPartitioning.numPartitions)
      val expectedChildrenNumPartitions = if (nonShuffleChildrenNumPartitions.nonEmpty) {
        if (nonShuffleChildrenNumPartitions.length == childrenIndexes.length) {
          // Here we pick the max number of partitions among these non-shuffle children.
          nonShuffleChildrenNumPartitions.max
        } else {
          // Here we pick the max number of partitions among these non-shuffle children as the
          // expected number of shuffle partitions. However, if it's smaller than
          // `conf.numShufflePartitions`, we pick `conf.numShufflePartitions` as the
          // expected number of shuffle partitions.
          math.max(nonShuffleChildrenNumPartitions.max, conf.defaultNumShufflePartitions)
        }
      } else {
        childrenNumPartitions.max
      }

      val targetNumPartitions = requiredNumPartitions.getOrElse(expectedChildrenNumPartitions)

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, distribution), index) if childrenIndexes.contains(index) =>
          if (child.outputPartitioning.numPartitions == targetNumPartitions) {
            child
          } else {
            val defaultPartitioning = distribution.createPartitioning(targetNumPartitions)
            child match {
              // If child is an exchange, we replace it with a new one having defaultPartitioning.
              case ShuffleExchangeExec(_, c, _) => ShuffleExchangeExec(defaultPartitioning, c)
              case _ => ShuffleExchangeExec(defaultPartitioning, child)
            }
          }

        case ((child, _), _) => child
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

    operator.withNewChildren(children)
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan =>
      ensureDistributionAndOrdering(operator)
  }
}
