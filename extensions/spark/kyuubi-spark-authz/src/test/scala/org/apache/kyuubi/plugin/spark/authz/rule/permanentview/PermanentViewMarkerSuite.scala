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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project, View}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.KyuubiFunSuite

class PermanentViewMarkerSuite extends KyuubiFunSuite {

  private def newMarker(): PermanentViewMarker = {
    val desc = CatalogTable(
      identifier = TableIdentifier("v", Some("default")),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", "int").add("b", "string"),
      viewText = Some("SELECT a, b FROM t"))
    val child: LogicalPlan = View(desc, isTempView = false, LocalRelation($"a".int, $"b".string))
    PermanentViewMarker(child, desc)
  }

  test("new instance canonicalizes to the same plan as the original") {
    // PermanentViewMarker is a MultiInstanceRelation, so the analyzer may replace an occurrence
    // with newInstance() whenever the same relation appears twice in one plan. A new instance has
    // to keep sameResult with the original, otherwise CacheManager stops recognising it and the
    // view is read from its sources again.
    val marker = newMarker()
    assert(marker.newInstance().canonicalized == marker.canonicalized)
  }

  test("nested new instances canonicalize to the same plan as the original") {
    // newInstance() wraps the child in a Project, so calling it on a plan that is already a new
    // instance nests one Project inside another. Canonicalization has to see through every layer,
    // not just the first one.
    val marker = newMarker()
    val once = marker.newInstance().asInstanceOf[PermanentViewMarker]
    assert(once.newInstance().canonicalized == marker.canonicalized)
  }

  test("a marker over a plan that is not a View canonicalizes to the same plan") {
    // RuleApplyPermanentViewMarker also wraps the plan of every SubqueryExpression found inside
    // a view, and that plan is not a View. Canonicalization has to cover those markers too.
    val desc = CatalogTable(
      identifier = TableIdentifier("v", Some("default")),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", "int").add("b", "string"),
      viewText = Some("SELECT a, b FROM t WHERE a IN (SELECT a FROM s)"))
    val subqueryPlan: LogicalPlan =
      Project(Seq($"a".int, $"b".string), LocalRelation($"a".int, $"b".string))
    val marker = PermanentViewMarker(subqueryPlan, desc)
    assert(marker.newInstance().canonicalized == marker.canonicalized)
  }
}
