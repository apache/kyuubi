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

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.PermanentViewMarker

/**
 * Adding [[org.apache.kyuubi.plugin.spark.authz.util.PermanentViewMarker]] for permanent views
 * for marking catalogTable of views used by privilege checking
 * in [[org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization]].
 * [[org.apache.kyuubi.plugin.spark.authz.util.PermanentViewMarker]] must be transformed up later
 * in [[org.apache.kyuubi.plugin.spark.authz.util.RuleEliminateViewMarker]] optimizer.
 */
class RuleApplyPermanentViewMarker extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan mapChildren {
      case p: PermanentViewMarker => p
      case permanentView: View if hasResolvedPermanentView(permanentView) =>
        PermanentViewMarker(permanentView, permanentView.desc)
      case other => apply(other)
    }
  }
}
