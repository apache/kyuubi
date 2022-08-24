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

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.PermanentViewMarker

class ViewAccessAnalysis extends Rule[LogicalPlan] {

  val LOG = LogManager.getLogger(classOf[ViewAccessAnalysis])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    LOG.debug(s"ViewAccessAnalysis apply $plan")
    plan mapChildren {
      case p: PermanentViewMarker => p
      case permanentView if isPermanentView(permanentView) =>
        LOG.debug(s"isPermanentView yes, :$permanentView")
        applyPermanentViewMarker(permanentView)
      case other => apply(other)
    }
  }

  private def applyPermanentViewMarker(
      plan: LogicalPlan): LogicalPlan = {
    PermanentViewMarker(plan, plan.asInstanceOf[View].desc)
  }
}
