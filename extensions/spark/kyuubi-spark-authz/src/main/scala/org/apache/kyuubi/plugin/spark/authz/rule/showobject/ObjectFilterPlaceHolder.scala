package org.apache.kyuubi.plugin.spark.authz.rule.showobject

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

import org.apache.kyuubi.plugin.spark.authz.util.WithInternalChild

case class ObjectFilterPlaceHolder(child: LogicalPlan) extends UnaryNode
  with WithInternalChild {

  override def output: Seq[Attribute] = child.output

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    // `FilterDataSourceV2Strategy` requires child.nodename not changed
    if (child.nodeName == newChild.nodeName) {
      copy(newChild)
    } else {
      this
    }
  }
}
