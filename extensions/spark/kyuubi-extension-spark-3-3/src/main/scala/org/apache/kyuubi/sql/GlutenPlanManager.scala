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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UserDefinedExpression
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, CartesianProductExec}

/**
 * Kyuubi extension for gluten enabled case.
 * 1. check whether the plan contains too much unsupported operator
 */
case class GlutenPlanManager(session: SparkSession) extends ColumnarRule {
  private val GLUTEN_DRIVER_PLUGIN_CLASS = "io.glutenproject.GlutenDriverPlugin"

  override def preColumnarTransitions: Rule[SparkPlan] =
    if (sys.props.get("spark.testing").nonEmpty ||
      glutenEnabled(session.sparkContext.getConf)) {
      GlutenPlanAnalysis
    } else {
      (plan: SparkPlan) => plan
    }

  private def glutenEnabled(conf: SparkConf): Boolean = {
    conf.get("spark.plugins", "").contains(GLUTEN_DRIVER_PLUGIN_CLASS) &&
    conf.get("spark.gluten.enabled", "true") == "true"
  }
}

object GlutenPlanAnalysis extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    var count = 0

    plan foreach { p =>
      if (containsUnsupportedOperator(p)) {
        count += 1
      }
    }
    check(count)
    plan
  }

  private def containsUnsupportedOperator(plan: SparkPlan): Boolean = {
    plan match {
      case FileSourceScanExec(relation, _, _, _, _, _, _, _, _) =>
        !relation.fileFormat.isInstanceOf[ParquetFileFormat]
      case _: RowDataSourceScanExec =>
        true
      case _: UserDefinedExpression =>
        true
      case _: CartesianProductExec =>
        true
      case _: ShuffleExchangeExec =>
        true
      case _: CollectLimitExec =>
        true
      case _: ObjectHashAggregateExec =>
        true
      case _: SortAggregateExec =>
        true
      case _: CoalesceExec =>
        true
      case _: GenerateExec =>
        true
      case _: RangeExec =>
        true
      case _: SampleExec =>
        true
      case _: InMemoryTableScanExec =>
        true
      case _: BroadcastNestedLoopJoinExec =>
        true
      case _: SparkPlan =>
        // TODO check whether the plan contains unsupported expressions
        false
    }
  }

  /**
   * Check whether the count of un-support operator is over threshold.
   */
  private def check(count: Int): Unit = {
    if (count >= conf.getConf(KyuubiSQLConf.GLUTEN_FALLBACK_OPERATOR_THRESHOLD)) {
      throw TooMuchGlutenUnsupportedOperationException(
        s"Here contains too much Gluten un-support operators: $count," +
          s"recommend to disabled Gluten for this SQL or raise the threshold.")
    }
  }
}

final case class TooMuchGlutenUnsupportedOperationException(
    private val reason: String = "",
    private val cause: Throwable = None.orNull)
  extends KyuubiSQLExtensionException(reason, cause)
