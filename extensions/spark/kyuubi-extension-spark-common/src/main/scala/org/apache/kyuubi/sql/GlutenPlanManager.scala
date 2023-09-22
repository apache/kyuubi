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
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 */
case class GlutenPlanManager(session: SparkSession) extends ColumnarRule {
  private val GLUTEN_DRIVER_PLUGIN_CLASS = "io.glutenproject.GlutenDriverPlugin"

  val unSupportOperators = Option(System.getProperty("gluten.version")) match {
    case Some(version) if version == "1.1.0" =>
      Seq("")
    case None =>
      Seq("")
  }

  override def preColumnarTransitions: Rule[SparkPlan] =
    if (session.sparkContext.getConf.get("spark.plugins", "")
        .contains(GLUTEN_DRIVER_PLUGIN_CLASS)) {
      GlutenPlanAnalysis(unSupportOperators)
    } else {
      (plan: SparkPlan) => plan
    }
}

case class GlutenPlanAnalysis(unSupportOperators: Seq[String]) extends Rule[SparkPlan] {
  private var count = 0
  override def apply(plan: SparkPlan): SparkPlan = {
    // count un-supported plan
    if (unSupportOperators.contains(plan.getClass.getSimpleName)) {
      count += 1
    }

    // fallback to non-gluten mode
    if (count >= conf.getConf(KyuubiSQLConf.GLUTEN_FALLBACK_OPERATOR_THRESHOLD)) {
      throw TooMuchGlutenUnsupportedOperationException("")
    }
    plan
  }
}

final case class TooMuchGlutenUnsupportedOperationException(
    private val reason: String = "",
    private val cause: Throwable = None.orNull)
  extends KyuubiSQLExtensionException(reason, cause)
