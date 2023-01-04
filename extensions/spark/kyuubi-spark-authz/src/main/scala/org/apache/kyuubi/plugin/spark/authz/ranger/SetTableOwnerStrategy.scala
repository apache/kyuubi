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
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.RunnableCommand

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getAuthzUgi, getFieldVal, setFieldVal}
import org.apache.kyuubi.plugin.spark.authz.v2Commands

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
      planV1Cmd(r, tableId, authzUser, replace = false)
    case r: RunnableCommand
        if r.nodeName == "CreateViewCommand" && getFieldVal[ViewType](
          r,
          "viewType") == PersistedView =>
      val tableId = getFieldVal[TableIdentifier](r, "name")
      val replace = getFieldVal[Boolean](r, "replace")
      planV1Cmd(r, tableId, authzUser, replace)
    case p: V2CreateTablePlan if v2Commands.accept(p.nodeName) =>
      val cmd = v2Commands.withName(p.nodeName)
      val tableIdOpt = cmd.commandTypes.flatMap(cmdType =>
        v2Commands.getCmdOutputTableAndIdent(p, cmdType)._2).headOption
      val replace = cmd match {
        case v2Commands.ReplaceTable | v2Commands.ReplaceTableAsSelect =>
          true
        case _ =>
          false
      }
      tableIdOpt.map(tableId => planV2Cmd(p, tableId, authzUser, replace)).getOrElse(Nil)
    case _ =>
      Nil
  }

  private def planV1Cmd(
      plan: LogicalPlan,
      tableId: TableIdentifier,
      authzUser: String,
      replace: Boolean): Seq[SparkPlan] = {
    spark.sessionState.planner.plan(plan).map(exec =>
      SetTableOwnerExec(exec, tableId, authzUser, replace)).toSeq
  }

  private def planV2Cmd(
      plan: V2CreateTablePlan,
      tableId: Identifier,
      authzUser: String,
      replace: Boolean): Seq[SparkPlan] = {
    spark.sessionState.planner.plan(plan).map(exec =>
      SetV2TableOwnerExec(exec, tableId, authzUser, replace)).toSeq
  }
}

object SetTableOwnerStrategy {

  val KYUUBI_AUTHZ_OWNER_TAG = TreeNodeTag[Boolean]("__KYUUBI_AUTHZ_OWNER_TAG")
}
