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

import org.apache.kyuubi.plugin.spark.authz.util.{RuleEliminateMarker, RuleEliminateViewMarker}

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
    v1.injectResolutionRule(_ => new RuleReplaceShowObjectCommands())
    v1.injectResolutionRule(_ => new RuleApplyPermanentViewMarker())
    v1.injectResolutionRule(new RuleApplyRowFilterAndDataMasking(_))
    v1.injectOptimizerRule(_ => new RuleEliminateMarker())
    v1.injectOptimizerRule(new RuleAuthorization(_))
    v1.injectOptimizerRule(_ => new RuleEliminateViewMarker())
    v1.injectPlannerStrategy(new FilterDataSourceV2Strategy(_))
  }
}
