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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._

trait RepartitionBuilderWithRebalance extends RepartitionBuilder {
  override def buildRepartition(
      dynamicPartitionColumns: Seq[Attribute],
      query: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(KyuubiSQLConf.INFER_REBALANCE_AND_SORT_ORDERS) ||
      dynamicPartitionColumns.nonEmpty) {
      val adviceBytes = conf.getConf(KyuubiSQLConf.REBALANCE_ADVISORY_PARTITION_SIZE_IN_BYTES)
      RebalancePartitions(dynamicPartitionColumns, query, None, Option.apply(adviceBytes))
    } else {
      val maxColumns = conf.getConf(KyuubiSQLConf.INFER_REBALANCE_AND_SORT_ORDERS_MAX_COLUMNS)
      val inferred = InferRebalanceAndSortOrders.infer(query)
      if (inferred.isDefined) {
        val (partitioning, ordering) = inferred.get
        val rebalance = RebalancePartitions(partitioning.take(maxColumns), query)
        if (ordering.nonEmpty) {
          val sortOrders = ordering.take(maxColumns).map(o => SortOrder(o, Ascending))
          Sort(sortOrders, false, rebalance)
        } else {
          rebalance
        }
      } else {
        RebalancePartitions(dynamicPartitionColumns, query)
      }
    }
  }

  override def canInsertRepartitionByExpression(plan: LogicalPlan): Boolean = {
    super.canInsertRepartitionByExpression(plan) && {
      plan match {
        case _: RebalancePartitions => false
        case _ => true
      }
    }
  }
}

/**
 * For datasource table, there two commands can write data to table
 * 1. InsertIntoHadoopFsRelationCommand
 * 2. CreateDataSourceTableAsSelectCommand
 * This rule add a RebalancePartitions node between write and query
 */
case class RebalanceBeforeWritingDatasource(session: SparkSession)
  extends RepartitionBeforeWritingDatasourceBase
  with RepartitionBuilderWithRebalance {}

/**
 * For Hive table, there two commands can write data to table
 * 1. InsertIntoHiveTable
 * 2. CreateHiveTableAsSelectCommand
 * This rule add a RebalancePartitions node between write and query
 */
case class RebalanceBeforeWritingHive(session: SparkSession)
  extends RepartitionBeforeWritingHiveBase
  with RepartitionBuilderWithRebalance {}
