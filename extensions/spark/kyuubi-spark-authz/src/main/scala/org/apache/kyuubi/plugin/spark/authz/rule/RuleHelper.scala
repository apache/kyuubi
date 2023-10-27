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
