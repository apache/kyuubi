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

package org.apache.kyuubi.plugin.spark.authz.gen

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._
import org.apache.kyuubi.plugin.spark.authz.serde._

object DeltaCommands extends CommandSpecs[TableCommandSpec] {

  val DeleteCommand = {
    val cmd = "org.apache.spark.sql.delta.commands.DeleteCommand"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE), comment = "Delta")
    val tableDesc = TableDesc(
      "target",
      classOf[SubqueryAliasTableExtractor],
      actionTypeDesc = Some(actionTypeDesc),
      comment = "Delta")
    val uriDescs = Seq(UriDesc("target", classOf[SubqueryAliasURIExtractor], comment = "Delta"))
    TableCommandSpec(cmd, Seq(tableDesc), uriDescs = uriDescs)
  }

  val UpdateCommand = {
    val cmd = "org.apache.spark.sql.delta.commands.UpdateCommand"
    DeleteCommand.copy(classname = cmd)
  }

  val MergeIntoCommand = {
    val cmd = "org.apache.spark.sql.delta.commands.MergeIntoCommand"
    val queryDesc = QueryDesc("source", comment = "Delta")
    DeleteCommand.copy(classname = cmd, queryDescs = Seq(queryDesc))
  }

  val OptimizeTableCommand = {
    val cmd = "org.apache.spark.sql.delta.commands.OptimizeTableCommand"
    val childDesc = TableDesc("child", classOf[ResolvedTableTableExtractor], comment = "Delta")
    val tableDesc =
      TableDesc("tableId", classOf[TableIdentifierOptionTableExtractor], comment = "Delta")
    val uriDescs = Seq(
      UriDesc("child", classOf[ResolvedTableURIExtractor], comment = "Delta"),
      UriDesc("tableId", classOf[TableIdentifierOptionURIExtractor], comment = "Delta"),
      UriDesc("path", classOf[StringURIExtractor], comment = "Delta"))
    TableCommandSpec(cmd, Seq(childDesc, tableDesc), ALTERTABLE_COMPACT, uriDescs = uriDescs)
  }

  val VacuumTableCommand = {
    val cmd = "io.delta.tables.execution.VacuumTableCommand"
    val childDesc = TableDesc("child", classOf[ResolvedTableTableExtractor], comment = "Delta")
    val tableDesc =
      TableDesc("table", classOf[TableIdentifierOptionTableExtractor], comment = "Delta")
    val uriDescs = Seq(
      UriDesc("child", classOf[ResolvedTableURIExtractor], comment = "Delta"),
      UriDesc("table", classOf[TableIdentifierOptionURIExtractor], comment = "Delta"),
      UriDesc("path", classOf[StringURIExtractor], comment = "Delta"))
    TableCommandSpec(cmd, Seq(childDesc, tableDesc), MSCK, uriDescs = uriDescs)
  }

  override def specs: Seq[TableCommandSpec] = Seq(
    DeleteCommand,
    MergeIntoCommand,
    OptimizeTableCommand,
    UpdateCommand,
    VacuumTableCommand)
}
