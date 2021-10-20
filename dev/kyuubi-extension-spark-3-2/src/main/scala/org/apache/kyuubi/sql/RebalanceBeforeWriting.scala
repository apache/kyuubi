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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._

trait RepartitionBuilderWithRebalance extends RepartitionBuilder {
  override def buildRepartition(
      dynamicPartitionColumns: Seq[Attribute], query: LogicalPlan): LogicalPlan = {
    RebalancePartitions(dynamicPartitionColumns, query)
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
    with RepartitionBuilderWithRebalance {
}

/**
 * For Hive table, there two commands can write data to table
 * 1. InsertIntoHiveTable
 * 2. CreateHiveTableAsSelectCommand
 * This rule add a RebalancePartitions node between write and query
 */
case class RebalanceBeforeWritingHive(session: SparkSession)
  extends RepartitionBeforeWritingHiveBase
    with RepartitionBuilderWithRebalance {
}

