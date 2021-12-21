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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExpressionSet, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.types.StructType

trait PruneFileSourcePartitionHelper extends PredicateHelper {

  def getPartitionKeyFiltersAndDataFilters(
      sparkSession: SparkSession,
      relation: LeafNode,
      partitionSchema: StructType,
      filters: Seq[Expression],
      output: Seq[AttributeReference]): (ExpressionSet, Seq[Expression]) = {
    val normalizedFilters = DataSourceStrategy.normalizeExprs(
      filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f)),
      output)
    val partitionColumns =
      relation.resolve(partitionSchema, sparkSession.sessionState.analyzer.resolver)
    val partitionSet = AttributeSet(partitionColumns)
    val (partitionFilters, dataFilters) = normalizedFilters.partition(f =>
      f.references.subsetOf(partitionSet))
    val extraPartitionFilter =
      dataFilters.flatMap(extractPredicatesWithinOutputSet(_, partitionSet))

    (ExpressionSet(partitionFilters ++ extraPartitionFilter), dataFilters)
  }
}
