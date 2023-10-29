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

package org.apache.kyuubi.plugin.spark.authz.rule

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.serde.{getTableCommandSpec, isKnownTableCommand}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils

trait RuleHelper extends Rule[LogicalPlan] {

  def spark: SparkSession

  final protected val parse: String => Expression = spark.sessionState.sqlParser.parseExpression _

  protected def mapChildren(plan: LogicalPlan)(f: LogicalPlan => LogicalPlan): LogicalPlan = {
    val newChildren = plan match {
      case cmd if isKnownTableCommand(cmd) =>
        val tableCommandSpec = getTableCommandSpec(cmd)
        val queries = tableCommandSpec.queries(cmd)
        cmd.children.map {
          case c if queries.contains(c) => f(c)
          case other => other
        }
      case _ =>
        plan.children.map(f)
    }
    plan.withNewChildren(newChildren)
  }

  def ugi: UserGroupInformation = AuthZUtils.getAuthzUgi(spark.sparkContext)

}
