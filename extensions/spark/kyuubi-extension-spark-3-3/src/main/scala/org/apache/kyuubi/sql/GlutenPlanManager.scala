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
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, CartesianProductExec}

/**
 * [Experimental]Kyuubi extension for gluten enabled case.
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
    val count = plan.collect {
      case p: FileSourceScanExec
          if !p.relation.fileFormat.isInstanceOf[ParquetFileFormat] =>
        true
      case _: RowDataSourceScanExec |
          _: UserDefinedExpression |
          _: CartesianProductExec |
          _: ShuffleExchangeExec |
          _: ObjectHashAggregateExec |
          _: SortAggregateExec |
          _: CoalesceExec |
          _: GenerateExec |
          _: RangeExec |
          _: SampleExec |
          _: BroadcastNestedLoopJoinExec =>
        true
      // TODO check whether the plan contains unsupported expressions
    }.size
    check(count)
    plan
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
