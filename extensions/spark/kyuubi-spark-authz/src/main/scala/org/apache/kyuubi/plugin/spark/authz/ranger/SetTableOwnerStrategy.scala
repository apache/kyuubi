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

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{PersistedView, ViewType}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.RunnableCommand

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getAuthzUgi, getFieldVal, setFieldVal}

class SetTableOwnerStrategy(spark: SparkSession) extends Strategy {
  import SetTableOwnerStrategy._

  private val commandToTableField = Map(
    "CreateDataSourceTableCommand" -> "table",
    "CreateDataSourceTableAsSelectCommand" -> "table",
    "CreateTableCommand" -> "table",
    "CreateHiveTableAsSelectCommand" -> "tableDesc",
    "OptimizedCreateHiveTableAsSelectCommand" -> "tableDesc")

  private val authzUser = getAuthzUgi(spark.sparkContext).getShortUserName

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case c: Command if commandToTableField.contains(c.nodeName) =>
      val field = commandToTableField(c.nodeName)
      val table = getFieldVal[CatalogTable](plan, field)
      if (table.owner != authzUser) {
        setFieldVal(plan, field, table.copy(owner = authzUser))
      }
      Nil
    case p if !plan.getTagValue(KYUUBI_AUTHZ_OWNER_TAG).contains(true) =>
      p.setTagValue(KYUUBI_AUTHZ_OWNER_TAG, true)
      applyInternal(p)
    case _ =>
      Nil
  }

  private def applyInternal(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case r: RunnableCommand if r.nodeName == "CreateTableLikeCommand" =>
      val tableId = getFieldVal[TableIdentifier](r, "targetTable")
      planInternal(r, tableId, authzUser, replace = false)
    case r: RunnableCommand
        if r.nodeName == "CreateViewCommand" && getFieldVal[ViewType](
          r,
          "viewType") == PersistedView =>
      val tableId = getFieldVal[TableIdentifier](r, "name")
      val replace = getFieldVal[Boolean](r, "replace")
      planInternal(r, tableId, authzUser, replace)
    case _ =>
      Nil
  }

  private def planInternal(
      plan: LogicalPlan,
      tableId: TableIdentifier,
      authzUser: String,
      replace: Boolean): Seq[SparkPlan] = {
    spark.sessionState.planner.plan(plan).map(exec =>
      SetTableOwnerExec(exec, tableId, authzUser, replace)).toSeq
  }
}

object SetTableOwnerStrategy {

  val KYUUBI_AUTHZ_OWNER_TAG = TreeNodeTag[Boolean]("__KYUUBI_AUTHZ_OWNER_TAG")
}
