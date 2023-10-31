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

package org.apache.kyuubi.plugin.spark.authz.rule

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.kyuubi.plugin.spark.authz.rule.Authorization._
import org.apache.kyuubi.plugin.spark.authz.rule.permanentview.PermanentViewMarker

abstract class Authorization(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case plan if isAuthChecked(plan) => plan // do nothing if checked privileges already.
      case p =>
        checkPrivileges(spark, p)
        markAuthChecked(p)
    }
  }

  def checkPrivileges(spark: SparkSession, plan: LogicalPlan): Unit
}

object Authorization {

  val KYUUBI_AUTHZ_TAG = TreeNodeTag[Unit]("__KYUUBI_AUTHZ_TAG")

  protected def markAuthChecked(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _: PermanentViewMarker =>
        plan.transformUp { case p =>
          p.setTagValue(KYUUBI_AUTHZ_TAG, ())
          p
        }
      case _ =>
        plan.setTagValue(KYUUBI_AUTHZ_TAG, ())
    }
    plan
  }

  protected def isAuthChecked(plan: LogicalPlan): Boolean = {
    plan.find(_.getTagValue(KYUUBI_AUTHZ_TAG).nonEmpty).nonEmpty
  }
}
