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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.kyuubi.plugin.spark.authz.ranger.RuleRelationAuthorization.KYUUBI_AUTHZ_TAG

class RuleRelationAuthorization(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case p if !plan.getTagValue(KYUUBI_AUTHZ_TAG).contains(true) =>
      RuleRelationAuthorization.checkPrivileges(spark, p)
      p.setTagValue(KYUUBI_AUTHZ_TAG, true)
      p
    case p => p // do nothing if checked privileges already.
  }
}

object RuleRelationAuthorization extends RuleAuthorizationProvider {

  val KYUUBI_AUTHZ_TAG = TreeNodeTag[Boolean]("__KYUUBI_AUTHZ_TAG")

}
