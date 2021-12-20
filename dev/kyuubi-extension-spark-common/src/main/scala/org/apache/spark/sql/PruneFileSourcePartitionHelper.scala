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
