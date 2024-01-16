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

package org.apache.kyuubi.plugin.spark.authz.rule.permanentview

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, Statistics, View}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

case class PermanentViewMarker(child: LogicalPlan, catalogTable: CatalogTable)
  extends LeafNode with MultiInstanceRelation {

  private val PVM_NEW_INSTANCE_TAG = TreeNodeTag[Unit]("__PVM_NEW_INSTANCE_TAG")

  override def output: Seq[Attribute] = child.output

  override def argString(maxFields: Int): String = ""

  override def innerChildren: Seq[QueryPlan[_]] = child :: Nil

  override def computeStats(): Statistics = child.stats

  override def newInstance(): LogicalPlan = {
    val projectList = child.output.map { case attr =>
      Alias(Cast(attr, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
    }
    val newProj = Project(projectList, child)
    newProj.setTagValue(PVM_NEW_INSTANCE_TAG, ())

    this.copy(child = newProj, catalogTable = catalogTable)
  }

  override def doCanonicalize(): LogicalPlan = {
    child match {
      case p @ Project(_, view: View) if p.getTagValue(PVM_NEW_INSTANCE_TAG).contains(true) =>
        view.canonicalized
      case _ =>
        child.canonicalized
    }
  }
}
