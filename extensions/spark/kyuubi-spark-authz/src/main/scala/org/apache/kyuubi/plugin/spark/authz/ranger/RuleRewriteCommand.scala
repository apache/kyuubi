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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.kyuubi.plugin.spark.authz.serde.{DB_COMMAND_SPECS, LogicalPlanRewriter, TABLE_COMMAND_SPECS}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.getAuthzUgi
import org.apache.kyuubi.plugin.spark.authz.util.WithInternalChildren

class RuleRewriteCommand(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val spec = TABLE_COMMAND_SPECS.get(plan.getClass.getName).orElse(
      DB_COMMAND_SPECS.get(plan.getClass.getName))
    spec.flatMap(_.logicalPlanRewriter).flatMap(
      LogicalPlanRewriter.logicalPlanRewriters.get) match {
      case Some(rewriter) =>
        rewriter.rewrite(spark, plan)
      case None =>
        plan
    }
  }
}

case class RewriteTableOwnerRunnableCommand(
    delegate: RunnableCommand,
    tableId: TableIdentifier)
  extends RunnableCommand with WithInternalChildren {

  override lazy val metrics: Map[String, SQLMetric] = delegate.metrics

  override val output: Seq[Attribute] = delegate.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ret = delegate.run(sparkSession)

    val catalog = sparkSession.sessionState.catalog
    val metadata = catalog.getTableMetadata(tableId)
    val authzUser = getAuthzUgi(sparkSession.sparkContext).getShortUserName
    if (metadata.owner != authzUser) {
      catalog.alterTable(metadata.copy(owner = authzUser))
    }
    ret
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}
