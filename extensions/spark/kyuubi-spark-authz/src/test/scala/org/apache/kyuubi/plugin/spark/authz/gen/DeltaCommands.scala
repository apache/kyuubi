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

import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._
import org.apache.kyuubi.plugin.spark.authz.serde._

object DeltaCommands extends CommandSpecs[TableCommandSpec] {

  val DeleteCommand = {
    val cmd = "org.apache.spark.sql.delta.commands.DeleteCommand"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc = TableDesc(
      "catalogTable",
      classOf[CatalogTableOptionTableExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc))
    val targetDesc = TableDesc(
      "target",
      classOf[SubqueryAliasTableExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc, targetDesc))
  }

  val UpdateCommand = {
    val cmd = "org.apache.spark.sql.delta.commands.UpdateCommand"
    DeleteCommand.copy(classname = cmd)
  }

  val MergeIntoCommand = {
    val cmd = "org.apache.spark.sql.delta.commands.MergeIntoCommand"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc = TableDesc(
      "target",
      classOf[SubqueryAliasTableExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    val queryDesc = QueryDesc("source")
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDesc))
  }

  override def specs: Seq[TableCommandSpec] = Seq(
    DeleteCommand,
    MergeIntoCommand,
    UpdateCommand)
}
