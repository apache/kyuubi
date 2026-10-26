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

package org.apache.kyuubi.plugin.spark.authz.rule.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{ResetCommand, SetCommand}

import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.SKIP_CATALOGLESS_V2_RELATION_ENABLED_KEY

/**
 * For banning end-users from set restricted spark configurations
 */
case class AuthzConfigurationChecker(spark: SparkSession) extends (LogicalPlan => Unit) {

  final val RESTRICT_LIST_KEY = "spark.kyuubi.conf.restricted.list"
  final val EXCLUDED_RULES_KEY = "spark.sql.optimizer.excludedRules"

  // Every rule the extension injects lives under this package, not only the ranger ones:
  // the marker-eliminating rules sit in org.apache.kyuubi.plugin.spark.authz.rule, and new
  // rules land there too. Excluding one of those breaks the query at planning today rather
  // than lifting a check, but the denylist should not depend on that staying true.
  final private val AUTHZ_RULE_PACKAGE = "org.apache.kyuubi.plugin.spark.authz"

  private val restrictedConfList: Set[String] =
    Set(
      RESTRICT_LIST_KEY,
      "spark.sql.runSQLOnFiles",
      "spark.sql.extensions",
      SKIP_CATALOGLESS_V2_RELATION_ENABLED_KEY) ++
      spark.conf.getOption(RESTRICT_LIST_KEY).map(_.split(',').toSet).getOrElse(Set.empty)

  override def apply(plan: LogicalPlan): Unit = {
    // SET is not the only way the exclusion can be set: spark.conf.set and the Spark
    // Connect Config RPC write the value with no logical plan at all, so the SetCommand
    // case below never sees them. Check the value that is actually in effect on every
    // plan instead - check rules are not affected by spark.sql.optimizer.excludedRules,
    // which only filters optimizer batches, so this check cannot be turned off the same way.
    if (spark.conf.getOption(EXCLUDED_RULES_KEY).exists(_.contains(AUTHZ_RULE_PACKAGE))) {
      throw new AccessControlException("Excluding Authz security rules is not allowed")
    }
    plan match {
      case SetCommand(Some((
            EXCLUDED_RULES_KEY,
            Some(v)))) if v.contains(AUTHZ_RULE_PACKAGE) =>
        throw new AccessControlException("Excluding Authz security rules is not allowed")
      case SetCommand(Some((k, Some(_)))) if restrictedConfList.contains(k) =>
        throw new AccessControlException(s"Modifying config $k is not allowed")
      case ResetCommand(Some(k)) if restrictedConfList.contains(k) =>
        throw new AccessControlException(s"Resetting config $k is not allowed")
      case ResetCommand(None) =>
        throw new AccessControlException("Resetting all configs is not allowed")
      case _ =>
    }
  }
}
