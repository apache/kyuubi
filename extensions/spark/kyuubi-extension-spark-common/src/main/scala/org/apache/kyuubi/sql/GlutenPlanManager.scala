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
import org.apache.spark.sql.execution.{ColumnarRule, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

/**
 */
case class GlutenPlanManager(session: SparkSession) extends ColumnarRule {
  private val GLUTEN_DRIVER_PLUGIN_CLASS = "io.glutenproject.GlutenDriverPlugin"

  override def preColumnarTransitions: Rule[SparkPlan] =
    if (glutenEnabled(session.sparkContext.getConf)) {
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
  private val unSupportExpressions = Seq(
    "CaseWhen",
    "BitAndAgg",
    "BitOrAgg",
    "BitXorAgg",
    "Base64",
    "Bin",
    "BitLength",
    "Contains")

  override def apply(plan: SparkPlan): SparkPlan = {
    var count = 0

    plan foreach { p =>
      if (containsUnsupportedOperator(p)) {
        count += 1
        check(count)
      }
    }
    plan
  }

  private def containsUnsupportedOperator(plan: SparkPlan): Boolean = {
    plan match {
      case FileSourceScanExec(relation, _, _, _, _, _, _, _, _) =>
        !relation.fileFormat.isInstanceOf[ParquetFileFormat]
      case _: UserDefinedExpression =>
        true
      case p: SparkPlan =>
        p.expressions.exists(e => unSupportExpressions.contains(e.getClass.getSimpleName))
    }
  }

  /**
   * Check whether the count of un-support operator is over threshold.
   */
  private def check(count: Int): Unit = {
    if (count >= conf.getConf(KyuubiSQLConf.GLUTEN_FALLBACK_OPERATOR_THRESHOLD)) {
      throw TooMuchGlutenUnsupportedOperationException("")
    }
  }
}

final case class TooMuchGlutenUnsupportedOperationException(
    private val reason: String = "",
    private val cause: Throwable = None.orNull)
  extends KyuubiSQLExtensionException(reason, cause)
