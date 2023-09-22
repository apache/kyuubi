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

import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
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
  private var count = 0
  private val unSupportExpressions = Seq(
    "CaseWhen",
    "BitAndAgg",
    "BitOrAgg",
    "BitXorAgg")
  override def apply(plan: SparkPlan): SparkPlan = {
    // count un-supported plan
    plan foreach {
      case FileSourceScanExec(relation, _, _, _, _, _, _, _, _)
          if !relation.fileFormat.isInstanceOf[ParquetFileFormat] =>
        count += 1
      // if plan has one un-support operator,
      // break to economize and mark as un-support
      case p: SparkPlan => breakable {
          p.expressions.foreach {
            case e: Expression if unSupportExpressions.contains(e.getClass.getSimpleName) =>
              incAndCheck()
            case _ =>
          }
        }
    }

    check()
    // do nothing with plan
    plan
  }

  private def incAndCheck(): Unit = {
    count += 1
    check()
    break
  }

  /**
   * Check whether the count of un-support operator is over threshold.
   */
  private def check(): Unit = {
    if (count >= conf.getConf(KyuubiSQLConf.GLUTEN_FALLBACK_OPERATOR_THRESHOLD)) {
      throw TooMuchGlutenUnsupportedOperationException("")
    }
  }
}

final case class TooMuchGlutenUnsupportedOperationException(
    private val reason: String = "",
    private val cause: Throwable = None.orNull)
  extends KyuubiSQLExtensionException(reason, cause)
