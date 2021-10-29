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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.sql.internal.StaticSQLConf

trait RepartitionBuilder extends Rule[LogicalPlan] with RepartitionBeforeWriteHelper {
  def buildRepartition(
      dynamicPartitionColumns: Seq[Attribute],
      query: LogicalPlan): LogicalPlan
}

/**
 * For datasource table, there two commands can write data to table
 * 1. InsertIntoHadoopFsRelationCommand
 * 2. CreateDataSourceTableAsSelectCommand
 * This rule add a repartition node between write and query
 */
abstract class RepartitionBeforeWritingDatasourceBase extends RepartitionBuilder {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE)) {
      addRepartition(plan)
    } else {
      plan
    }
  }

  private def addRepartition(plan: LogicalPlan): LogicalPlan = plan match {
    case i @ InsertIntoHadoopFsRelationCommand(_, sp, _, pc, bucket, _, _, query, _, _, _, _)
      if query.resolved && bucket.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns = pc.filterNot(attr => sp.contains(attr.name))
      i.copy(query = buildRepartition(dynamicPartitionColumns, query))

    case c @ CreateDataSourceTableAsSelectCommand(table, _, query, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns =
        query.output.filter(attr => table.partitionColumnNames.contains(attr.name))
      c.copy(query = buildRepartition(dynamicPartitionColumns, query))

    case _ => plan
  }
}

/**
 * For Hive table, there two commands can write data to table
 * 1. InsertIntoHiveTable
 * 2. CreateHiveTableAsSelectCommand
 * This rule add a repartition node between write and query
 */
abstract class RepartitionBeforeWritingHiveBase extends RepartitionBuilder {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION) == "hive" &&
      conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE)) {
      addRepartition(plan)
    } else {
      plan
    }
  }

  def addRepartition(plan: LogicalPlan): LogicalPlan = plan match {
    case i @ InsertIntoHiveTable(table, partition, query, _, _, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns = partition.filter(_._2.isEmpty).keys
        .flatMap(name => query.output.find(_.name == name)).toSeq
      i.copy(query = buildRepartition(dynamicPartitionColumns, query))

    case c @ CreateHiveTableAsSelectCommand(table, query, _, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns =
        query.output.filter(attr => table.partitionColumnNames.contains(attr.name))
      c.copy(query = buildRepartition(dynamicPartitionColumns, query))

    case c @ OptimizedCreateHiveTableAsSelectCommand(table, query, _, _)
      if query.resolved && table.bucketSpec.isEmpty && canInsertRepartitionByExpression(query) =>
      val dynamicPartitionColumns =
        query.output.filter(attr => table.partitionColumnNames.contains(attr.name))
      c.copy(query = buildRepartition(dynamicPartitionColumns, query))

    case _ => plan
  }
}

trait RepartitionBeforeWriteHelper {
  def canInsertRepartitionByExpression(plan: LogicalPlan): Boolean = plan match {
    case Project(_, child) => canInsertRepartitionByExpression(child)
    case Limit(_, _) => false
    case _: Sort => false
    case _: RepartitionByExpression => false
    case _: Repartition => false
    case _ => true
  }
}
