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

package org.apache.kyuubi.plugin.spark.authz.util

import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.util.ClassUtil.classOf
import org.apache.kyuubi.plugin.spark.authz.OperationType.{OperationType, QUERY}
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.PrivilegesBuilder.buildQuery
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getFieldVal, invoke}
import org.apache.kyuubi.plugin.spark.authz.v2Commands.CommandType.CommandType
import org.apache.kyuubi.plugin.spark.authz.v2Commands.{CommandType, v2TablePrivileges}
import org.apache.kyuubi.plugin.spark.authz.{OperationType, PrivilegeObject, PrivilegeObjectActionType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier

import scala.collection.mutable.ArrayBuffer

/**
 * Building privilege objects
 * for Spark Datasource arctic commands
 */
object ArcticCmdUtils {

  def operationType(arcticCmd: String): OperationType = {
    try {
      OperationType.withName(invoke(getArcticUtil,
        "operationType",
        (classOf[String], arcticCmd)).
        asInstanceOf[String])
    } catch {
      case _: Throwable => QUERY
    }
  }

  def buildInput(plan: LogicalPlan,
                 inputObjs: ArrayBuffer[PrivilegeObject],
                 commandTypes: Seq[CommandType]): Unit = {
    commandTypes.foreach {
      case CommandType.HasChildAsIdentifier =>
        val query = getFieldVal[LogicalPlan] (plan, "query")
        buildQuery (query, inputObjs)
      case _ =>
    }
  }

  def buildOutput(plan: LogicalPlan,
                  outputObjs: ArrayBuffer[PrivilegeObject],
                  commandTypes: Seq[CommandType],
                  outputObjsActionType: PrivilegeObjectActionType): Unit = {
    commandTypes.foreach {
      case CommandType.HasTableNameAsIdentifier =>
        val table = invoke(plan, "tableName").asInstanceOf[Identifier]
        outputObjs += v2TablePrivileges(table)

      case CommandType.HasTableAsIdentifierOption =>
        val table = getFieldVal[AnyRef](plan, "table")
        val tableIdent = getFieldVal[Option[Identifier]](table, "identifier")
        if (tableIdent.isDefined) {
          outputObjs += v2TablePrivileges(tableIdent.get, actionType = outputObjsActionType)
        }

      case CommandType.HasTableAsIdentifier =>
        val table = getFieldVal[LogicalPlan](plan, "table")
        val tableIdent = getFieldVal[Identifier](table, "identifier")
        outputObjs += v2TablePrivileges(tableIdent)

      case CommandType.HasChildAsIdentifier =>
        val table = getFieldVal[AnyRef](plan, "child")
        val tableIdent = getFieldVal[Identifier](table, "identifier")
        outputObjs += v2TablePrivileges(tableIdent)

      case _ =>
    }

  }

  def buildCommandTypes(cmd: String): Seq[CommandType] = {
    try {
      val types = invoke(getArcticUtil,
        "commandType",
        (classOf[String], cmd)).
        asInstanceOf[Seq[String]]
      types.map(f => CommandType.withName(f))
    } catch {
      case _: Throwable => Seq.empty
    }
  }

  def buildOutputActionType(cmd: String): PrivilegeObjectActionType = {
    try {
      PrivilegeObjectActionType.withName(invoke(getArcticUtil,
        "actionType",
        (classOf[String], cmd)).
        asInstanceOf[String])
    } catch {
      case _: Throwable => PrivilegeObjectActionType.OTHER
    }
  }


  def getArcticUtil: AnyRef = {
    Class.forName("com.netease.arctic.spark.sql.utils.ArcticAuthUtil")
  }

  def buildPrivileges(
                       cmd: String,
                       plan: LogicalPlan,
                       inputObjs: ArrayBuffer[PrivilegeObject],
                       outputObjs: ArrayBuffer[PrivilegeObject]): Unit = {
    val commandTypes = buildCommandTypes(cmd)
    val outputActionType = buildOutputActionType(cmd)

    buildInput(plan, inputObjs, commandTypes)
    buildOutput(plan, outputObjs, commandTypes, outputActionType)
  }


  def accept(commandName: String): Boolean = {
    try {
      invoke(
        Class.forName("com.netease.arctic.spark.sql.utils.ArcticAuthUtil"),
        "isArcticCommand", (Boolean.getClass, commandName)).asInstanceOf[Boolean]
    } catch {
      case _: Throwable => false
    }
  }
}
