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
package org.apache.kyuubi.plugin.spark.authz.rule.rowfilter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SparkStrategy

case class FilterDataSourceV2Strategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // For Spark 3.1 and below, `ColumnPruning` rule will set `ObjectFilterPlaceHolder#child` to
    // `Project`
    case ObjectFilterPlaceHolder(Project(_, child)) if child.nodeName == "ShowNamespaces" =>
      spark.sessionState.planner.plan(child)
        .map(FilteredShowNamespaceExec(_, spark.sparkContext)).toSeq

    // For Spark 3.2 and above
    case ObjectFilterPlaceHolder(child) if child.nodeName == "ShowNamespaces" =>
      spark.sessionState.planner.plan(child)
        .map(FilteredShowNamespaceExec(_, spark.sparkContext)).toSeq

    case ObjectFilterPlaceHolder(child) if child.nodeName == "ShowTables" =>
      spark.sessionState.planner.plan(child)
        .map(FilteredShowTablesExec(_, spark.sparkContext)).toSeq

    case _ => Nil
  }
}
