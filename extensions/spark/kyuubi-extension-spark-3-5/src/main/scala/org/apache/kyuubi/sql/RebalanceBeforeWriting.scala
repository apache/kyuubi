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

import scala.annotation.tailrec

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.internal.StaticSQLConf

trait RebalanceBeforeWritingBase extends Rule[LogicalPlan] {

  private def hasBenefit(plan: LogicalPlan): Boolean = {
    def probablyHasShuffle: Boolean = plan.exists {
      case _: Join => true
      case _: Aggregate => true
      case _: Distinct => true
      case _: Deduplicate => true
      case _: Window => true
      case s: Sort if s.global => true
      case _: RepartitionOperation => true
      case _: GlobalLimit => true
      case _ => false
    }

    conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE) || probablyHasShuffle
  }

  def canInsertRebalance(plan: LogicalPlan): Boolean = {
    @tailrec
    def canInsert(p: LogicalPlan): Boolean = p match {
      case Project(_, child) => canInsert(child)
      case SubqueryAlias(_, child) => canInsert(child)
      case Limit(_, _) => false
      case _: Sort => false
      case _: RepartitionByExpression => false
      case _: Repartition => false
      case _: RebalancePartitions => false
      case _ => true
    }

    // 1. make sure AQE is enabled, otherwise it is no meaning to add a shuffle
    // 2. make sure it does not break the semantics of original plan
    // 3. try to avoid adding a shuffle if it has potential performance regression
    conf.adaptiveExecutionEnabled && canInsert(plan) && hasBenefit(plan)
  }

  def buildRebalance(
      dynamicPartitionColumns: Seq[Attribute],
      query: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(KyuubiSQLConf.INFER_REBALANCE_AND_SORT_ORDERS) ||
      dynamicPartitionColumns.nonEmpty) {
      RebalancePartitions(dynamicPartitionColumns, query)
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
}

/**
 * For datasource table, there two commands can write data to table
 * 1. InsertIntoHadoopFsRelationCommand
 * 2. CreateDataSourceTableAsSelectCommand
 * This rule add a repartition node between write and query
 */
case class RebalanceBeforeWritingDatasource(session: SparkSession)
  extends RebalanceBeforeWritingBase {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE)) {
      addRebalance(plan)
    } else {
      plan
    }
  }

  private def addRebalance(plan: LogicalPlan): LogicalPlan = plan match {
    case i @ InsertIntoHadoopFsRelationCommand(_, sp, _, pc, bucket, _, _, query, _, _, _, _)
        if query.resolved && bucket.isEmpty && canInsertRebalance(query) =>
      val dynamicPartitionColumns = pc.filterNot(attr => sp.contains(attr.name))
      i.copy(query = buildRebalance(dynamicPartitionColumns, query))

    case u @ Union(children, _, _) =>
      u.copy(children = children.map(addRebalance))

    case _ => plan
  }
}

/**
 * For Hive table, there two commands can write data to table
 * 1. InsertIntoHiveTable
 * 2. CreateHiveTableAsSelectCommand
 * This rule add a repartition node between write and query
 */
case class RebalanceBeforeWritingHive(session: SparkSession)
  extends RebalanceBeforeWritingBase {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive" &&
      conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE)) {
      addRebalance(plan)
    } else {
      plan
    }
  }

  private def addRebalance(plan: LogicalPlan): LogicalPlan = plan match {
    case i @ InsertIntoHiveTable(table, partition, query, _, _, _, _, _, _, _, _)
        if query.resolved && table.bucketSpec.isEmpty && canInsertRebalance(query) =>
      val dynamicPartitionColumns = partition.filter(_._2.isEmpty).keys
        .flatMap(name => query.output.find(_.name == name)).toSeq
      i.copy(query = buildRebalance(dynamicPartitionColumns, query))

    case u @ Union(children, _, _) =>
      u.copy(children = children.map(addRebalance))

    case _ => plan
  }
}
