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

import java.lang.reflect.{InvocationTargetException, Method}

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ShuffleOrigin}

/**
 * Spark `EnsureRequirements` without reordering join predicates and pruning shuffles.
 *
 * Spark's `EnsureRequirements.apply` reorders join predicates and prunes redundant
 * repartitions before ensuring each operator's child distribution and ordering. This rule
 * only does the latter, by calling Spark's private `ensureDistributionAndOrdering`, so it
 * keeps up with the Spark runtime instead of carrying a copy of it.
 */
object KyuubiEnsureRequirements extends Rule[SparkPlan] {

  private val METHOD_NAME = "ensureDistributionAndOrdering"

  // The method reads no `EnsureRequirements` state, so any instance can serve as the receiver.
  private lazy val ensureRequirements = EnsureRequirements()

  private lazy val ensureDistributionAndOrderingMethod: Method = {
    val clazz = classOf[EnsureRequirements]
    val argClasses = Seq(
      classOf[Option[_]],
      classOf[Seq[_]],
      classOf[Seq[_]],
      classOf[Seq[_]],
      classOf[ShuffleOrigin])
    // scalac makes a private method public, under its expanded name, when a nested lambda
    // calls it; otherwise it stays private under its plain name.
    Try(clazz.getMethod(s"${clazz.getName.replace('.', '$')}$$$$$METHOD_NAME", argClasses: _*))
      .orElse(Try {
        val method = clazz.getDeclaredMethod(METHOD_NAME, argClasses: _*)
        method.setAccessible(true)
        method
      })
      .getOrElse(throw new KyuubiSQLExtensionException(
        s"Failed to find ${clazz.getName}.$METHOD_NAME in the Spark runtime"))
  }

  private def ensureDistributionAndOrdering(
      parent: Option[SparkPlan],
      originalChildren: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution],
      requiredChildOrderings: Seq[Seq[SortOrder]],
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = {
    try {
      ensureDistributionAndOrderingMethod.invoke(
        ensureRequirements,
        parent,
        originalChildren,
        requiredChildDistributions,
        requiredChildOrderings,
        shuffleOrigin).asInstanceOf[Seq[SparkPlan]]
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan =>
      val newChildren = ensureDistributionAndOrdering(
        Some(operator),
        operator.children,
        operator.requiredChildDistribution,
        operator.requiredChildOrdering,
        ENSURE_REQUIREMENTS)
      operator.withNewChildren(newChildren)
  }
}
