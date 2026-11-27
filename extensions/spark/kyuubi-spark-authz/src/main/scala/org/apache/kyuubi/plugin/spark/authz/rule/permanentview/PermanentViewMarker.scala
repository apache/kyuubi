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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, Statistics, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.kyuubi.util.reflect.ReflectUtils.getField

case class PermanentViewMarker(child: LogicalPlan, catalogTable: CatalogTable)
  extends LeafNode with MultiInstanceRelation {

  private val PVM_NEW_INSTANCE_TAG = TreeNodeTag[Unit]("__PVM_NEW_INSTANCE_TAG")

  override def output: Seq[Attribute] = child.output

  override def argString(maxFields: Int): String = ""

  override def innerChildren: Seq[QueryPlan[_]] = child :: Nil

  override def computeStats(): Statistics = child.stats

  // As a LeafNode the marker has no expressions or children, so QueryPlan.deterministic would
  // report true even for a view over rand() or uuid(). InlineCTE relies on this flag to keep a
  // nondeterministic CTE out of line when it is referenced more than once.
  override lazy val deterministic: Boolean = child.deterministic

  override def newInstance(): LogicalPlan = {
    val projectList = child.output.map { case attr =>
      Alias(attr, attr.name)(explicitMetadata = Some(attr.metadata))
    }
    val newProj = Project(projectList, child)
    newProj.setTagValue(PVM_NEW_INSTANCE_TAG, ())

    this.copy(child = newProj, catalogTable = catalogTable)
  }

  override def doCanonicalize(): LogicalPlan = {
    val plan = canonicalizationPlan
    // Subquery markers can reference CTEs outside their child. Normalize them together with
    // the enclosing permanent view, which has its own CTE scope.
    val normalized = plan match {
      case _: View => PermanentViewMarker.normalizeCTEIds.map(_(plan)).getOrElse(plan)
      case _ => plan
    }
    normalized.canonicalized
  }

  private def canonicalizationPlan: LogicalPlan = {
    // newInstance() wraps the child in a Project, and a new instance of a new instance nests one
    // inside another, so strip every layer it added rather than only the outermost one.
    @tailrec
    def stripNewInstanceProjects(plan: LogicalPlan): LogicalPlan = plan match {
      case p: Project if p.getTagValue(PVM_NEW_INSTANCE_TAG).isDefined =>
        stripNewInstanceProjects(p.child)
      case other => other
    }
    // Expose nested views and subqueries only for cache-key comparison. The analyzed plan
    // must retain its markers for privilege checks.
    stripNewInstanceProjects(child).transformDownWithSubqueries {
      case marker: PermanentViewMarker => marker.canonicalizationPlan
    }
  }
}

object PermanentViewMarker {
  // SPARK-46741 introduced this rule in Spark 4.1.1. Keep older runtimes loadable and use
  // Spark's own normalization, including recursive CTE handling, wherever it is available.
  private lazy val normalizeCTEIds: Option[Rule[LogicalPlan]] = {
    try {
      Some(getField[Rule[LogicalPlan]](
        "org.apache.spark.sql.catalyst.normalizer.NormalizeCTEIds$",
        "MODULE$"))
    } catch {
      case _: ClassNotFoundException => None
    }
  }
}
