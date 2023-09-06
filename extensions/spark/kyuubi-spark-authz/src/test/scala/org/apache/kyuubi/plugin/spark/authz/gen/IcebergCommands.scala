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

object IcebergCommands {

  val DeleteFromIcebergTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DeleteFromIcebergTable"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc =
      TableDesc(
        "table",
        classOf[DataSourceV2RelationTableExtractor],
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(QueryDesc("query")))
  }

  val UpdateIcebergTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.UpdateIcebergTable"
    DeleteFromIcebergTable.copy(cmd)
  }

  val MergeIntoIcebergTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc = TableDesc(
      "targetTable",
      classOf[DataSourceV2RelationTableExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    val queryDesc = QueryDesc("sourceTable")
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDesc))
  }

  val IcebergCallCommand = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.Call"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc =
      TableDesc(
        "args",
        classOf[IcebergCallArgsTableExtractor],
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc))
  }

  val data: Array[TableCommandSpec] = Array(
    DeleteFromIcebergTable,
    UpdateIcebergTable,
    MergeIntoIcebergTable,
    MergeIntoIcebergTable.copy(classname =
      "org.apache.spark.sql.catalyst.plans.logical.UnresolvedMergeIntoIcebergTable"),
    IcebergCallCommand)
}
