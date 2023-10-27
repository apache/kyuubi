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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_NUM, ShuffleExchangeExec}
import org.apache.spark.sql.hive.HiveSparkPlanHelper.HiveTableScanExec
import org.apache.spark.sql.internal.SQLConf._

import org.apache.kyuubi.sql.KyuubiSQLConf.{DYNAMIC_SHUFFLE_PARTITIONS, DYNAMIC_SHUFFLE_PARTITIONS_MAX_NUM}

/**
 * Dynamically adjust the number of shuffle partitions according to the input data size
 */
case class DynamicShufflePartitions(spark: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(DYNAMIC_SHUFFLE_PARTITIONS)) {
      plan
    } else {
      val maxDynamicShufflePartitions = conf.getConf(DYNAMIC_SHUFFLE_PARTITIONS_MAX_NUM)

      def collectScanSizes(plan: SparkPlan): Seq[Long] = plan match {
        case FileSourceScanExec(relation, _, _, _, _, _, _, _, _) =>
          Seq(relation.location.sizeInBytes)
        case t: HiveTableScanExec =>
          t.relation.prunedPartitions match {
            case Some(partitions) => Seq(partitions.flatMap(_.stats).map(_.sizeInBytes.toLong).sum)
            case None => Seq(t.relation.computeStats().sizeInBytes.toLong)
                .filter(_ != conf.defaultSizeInBytes)
          }
        case stage: ShuffleQueryStageExec if stage.isMaterialized =>
          Seq(stage.mapStats.map(_.bytesByPartitionId.sum).getOrElse(0L))
        case p =>
          p.children.flatMap(collectScanSizes)
      }

      val targetSize = conf.getConf(ADVISORY_PARTITION_SIZE_IN_BYTES)
      val maxScanSizes = collectScanSizes(plan) match {
        case sizes if sizes.nonEmpty => sizes.max
        case _ => targetSize
      }
      val targetShufflePartitions = Math.min(
        Math.max(maxScanSizes / targetSize + 1, conf.numShufflePartitions).toInt,
        maxDynamicShufflePartitions)

      plan transformUp {
        case exchange @ ShuffleExchangeExec(outputPartitioning, _, shuffleOrigin, _)
            if shuffleOrigin != REPARTITION_BY_NUM =>
          val newOutPartitioning = outputPartitioning match {
            case RoundRobinPartitioning(numPartitions)
                if targetShufflePartitions != numPartitions =>
              Some(RoundRobinPartitioning(targetShufflePartitions))
            case HashPartitioning(expressions, numPartitions)
                if targetShufflePartitions != numPartitions =>
              Some(HashPartitioning(expressions, targetShufflePartitions))
            case RangePartitioning(ordering, numPartitions)
                if targetShufflePartitions != numPartitions =>
              Some(RangePartitioning(ordering, targetShufflePartitions))
            case _ => None
          }
          if (newOutPartitioning.isDefined) {
            exchange.copy(outputPartitioning = newOutPartitioning.get)
          } else {
            exchange
          }
      }
    }
  }

}
