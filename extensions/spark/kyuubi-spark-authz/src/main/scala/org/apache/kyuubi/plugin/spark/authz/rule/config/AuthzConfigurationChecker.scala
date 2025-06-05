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

import org.apache.spark.authz.AuthzConf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.SetCommand

import org.apache.kyuubi.plugin.spark.authz.AccessControlException

/**
 * For banning end-users from set restricted spark configurations
 */
case class AuthzConfigurationChecker(spark: SparkSession) extends (LogicalPlan => Unit) {

  private val restrictedConfList: Set[String] =
    Set(CONF_RESTRICTED_LIST.key, "spark.sql.runSQLOnFiles", "spark.sql.extensions") ++
      confRestrictedList(spark.sparkContext.getConf).map(_.split(',').toSet).getOrElse(Set.empty)

  override def apply(plan: LogicalPlan): Unit = plan match {
    case SetCommand(Some((
          "spark.sql.optimizer.excludedRules",
          Some(v)))) if v.contains("org.apache.kyuubi.plugin.spark.authz.ranger") =>
      throw new AccessControlException("Excluding Authz security rules is not allowed")
    case SetCommand(Some((k, Some(_)))) if restrictedConfList.contains(k) =>
      throw new AccessControlException(s"Modifying config $k is not allowed")
    case _ =>
  }
}
