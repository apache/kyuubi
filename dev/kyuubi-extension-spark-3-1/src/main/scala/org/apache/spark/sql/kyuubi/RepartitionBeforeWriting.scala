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

package org.apache.spark.sql.kyuubi

import java.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, Literal, Multiply, Rand}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

trait RepartitionBuilderWithRepartitionByExpression extends RepartitionBuilder {
  override def buildRepartition(
      dynamicPartitionColumns: Seq[Attribute], query: LogicalPlan): LogicalPlan = {
    if (dynamicPartitionColumns.isEmpty) {
      RepartitionByExpression(
        Seq.empty,
        query,
        conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_NUM))
    } else {
      val extended = dynamicPartitionColumns ++ dynamicPartitionExtraExpression(
        conf.getConf(KyuubiSQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM))
      RepartitionByExpression(
        extended,
        query,
        conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_NUM))
    }
  }

  def dynamicPartitionExtraExpression(partitionNumber: Int): Seq[Expression] = {
    // Dynamic partition insertion will add repartition by partition column, but it could cause
    // data skew (one partition value has large data). So we add extra partition column for the
    // same dynamic partition to avoid skew.
    Cast(Multiply(
      new Rand(Literal(new Random().nextLong())),
      Literal(partitionNumber.toDouble)
    ), IntegerType) :: Nil
  }
}

/**
 * For datasource table, there two commands can write data to table
 * 1. InsertIntoHadoopFsRelationCommand
 * 2. CreateDataSourceTableAsSelectCommand
 * This rule add a repartition node between write and query
 */
case class RepartitionBeforeWritingDatasource(session: SparkSession)
  extends RepartitionBeforeWritingDatasourceBase
    with RepartitionBuilderWithRepartitionByExpression {
}

/**
 * For Hive table, there two commands can write data to table
 * 1. InsertIntoHiveTable
 * 2. CreateHiveTableAsSelectCommand
 * This rule add a repartition node between write and query
 */
case class RepartitionBeforeWritingHive(session: SparkSession)
  extends RepartitionBeforeWritingHiveBase
    with RepartitionBuilderWithRepartitionByExpression {
}
