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

import org.apache.spark.sql.SparkSessionExtensions

import org.apache.kyuubi.plugin.spark.authz.rule.{RuleEliminateMarker, RuleEliminatePermanentViewMarker, RuleEliminateTypeOf}
import org.apache.kyuubi.plugin.spark.authz.rule.config.AuthzConfigurationChecker
import org.apache.kyuubi.plugin.spark.authz.rule.datamasking.{RuleApplyDataMaskingStage0, RuleApplyDataMaskingStage1}
import org.apache.kyuubi.plugin.spark.authz.rule.expression.RuleApplyTypeOfMarker
import org.apache.kyuubi.plugin.spark.authz.rule.permanentview.RuleApplyPermanentViewMarker
import org.apache.kyuubi.plugin.spark.authz.rule.rowfilter.{FilterDataSourceV2Strategy, RuleApplyRowFilter, RuleReplaceShowObjectCommands}

/**
 * ACL Management for Apache Spark SQL with Apache Ranger, enabling:
 * <ul>
 *   <li>Table/Column level authorization(yes)</li>
 *   <li>Row level filtering(yes)</li>
 *   <li>Data masking(yes)</li>
 * <ul>
 *
 * To work with Spark SQL, we need to enable it via spark extensions
 *
 * spark.sql.extensions=org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension
 *
 *  @since 1.6.0
 */
class RangerSparkExtension extends (SparkSessionExtensions => Unit) {
  SparkRangerAdminPlugin.initialize()

  override def apply(v1: SparkSessionExtensions): Unit = {
    v1.injectCheckRule(AuthzConfigurationChecker)
    // RuleFunctionAuthorization should use injectCheckRule instead of injectOptimizerRule,
    // because ConstantFolding will optimize deterministic UDFs with foldable
    // inputs (e.g., literals), replacing them with their results and bypassing permission checks.
    v1.injectCheckRule(RuleFunctionAuthorization)
    v1.injectResolutionRule(_ => RuleReplaceShowObjectCommands)
    v1.injectResolutionRule(_ => RuleApplyPermanentViewMarker)
    v1.injectResolutionRule(_ => RuleApplyTypeOfMarker)
    v1.injectResolutionRule(RuleApplyRowFilter)
    v1.injectResolutionRule(RuleApplyDataMaskingStage0)
    v1.injectResolutionRule(RuleApplyDataMaskingStage1)
    v1.injectOptimizerRule(_ => RuleEliminateMarker)
    v1.injectOptimizerRule(RuleAuthorization)
    v1.injectOptimizerRule(RuleEliminatePermanentViewMarker)
    v1.injectOptimizerRule(_ => RuleEliminateTypeOf)
    v1.injectPlannerStrategy(FilterDataSourceV2Strategy)
  }
}
