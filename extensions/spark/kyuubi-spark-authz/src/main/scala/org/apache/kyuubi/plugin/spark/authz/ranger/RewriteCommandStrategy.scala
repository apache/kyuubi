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

package org.apache.kyuubi.plugin.spark.authz.ranger

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan

import org.apache.kyuubi.plugin.spark.authz.ranger.RewriteCommandStrategy.KYUUBI_REWRITE_COMMAND_TAG
import org.apache.kyuubi.plugin.spark.authz.serde.{DB_COMMAND_SPECS, SparkPlanRewriter, TABLE_COMMAND_SPECS}

class RewriteCommandStrategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val spec = TABLE_COMMAND_SPECS.get(plan.getClass.getName).orElse(
      DB_COMMAND_SPECS.get(plan.getClass.getName))
    val rewriter =
      spec.flatMap(_.sparkPlanRewriter).flatMap(SparkPlanRewriter.sparkPlanRewriters.get)
    if (rewriter.nonEmpty
      && !plan.getTagValue(KYUUBI_REWRITE_COMMAND_TAG).contains(true)) {
      // Let SparkPlanner skip current strategy
      plan.setTagValue(KYUUBI_REWRITE_COMMAND_TAG, true)
      val execSeq =
        spark.sessionState.planner.plan(plan).map(exec => rewriter.get.rewrite(spark, exec)).toSeq
      plan.unsetTagValue(KYUUBI_REWRITE_COMMAND_TAG)
      execSeq
    } else {
      Nil
    }
  }
}

object RewriteCommandStrategy {
  val KYUUBI_REWRITE_COMMAND_TAG = TreeNodeTag[Boolean]("__KYUUBI_REWRITE_COMMAND_TAG")
}
