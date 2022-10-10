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

package org.apache.kyuubi.plugin.spark.authz

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.v2Commands.CommandType.CommandType

/**
 * Building privilege objects
 * for Iceberg commands rewritten by extension
 */
object IcebergCommands extends Enumeration {

  import scala.language.implicitConversions

  implicit def valueToCmdPrivilegeBuilder(x: Value): CmdPrivilegeBuilder =
    x.asInstanceOf[CmdPrivilegeBuilder]

  /**
   * check whether commandName is implemented with supported privilege builders
   * and pass the requirement checks (e.g. Spark version)
   *
   * @param commandName name of command
   * @return true if so, false else
   */
  def accept(commandName: String): Boolean = {
    try {
      val command = IcebergCommands.withName(commandName)

      // check spark version requirements
      passSparkVersionCheck(command.mostVer, command.leastVer)
    } catch {
      case _: NoSuchElementException => false
    }
  }

  def skipMappedChildren(plan: LogicalPlan): Seq[LogicalPlan] = {
    Seq(
      getFieldValOpt[LogicalPlan](plan, "table"),
      getFieldValOpt[LogicalPlan](plan, "targetTable"),
      getFieldValOpt[LogicalPlan](plan, "sourceTable"))
      .flatten intersect plan.children
  }

  /**
   * Command privilege builder
   *
   * @param operationType    OperationType for converting accessType
   * @param leastVer         minimum Spark version required
   * @param mostVer          maximum Spark version supported
   * @param commandTypes     Seq of [[CommandType]] hinting privilege building
   * @param buildInput       input [[PrivilegeObject]] for privilege check
   * @param buildOutput      output [[PrivilegeObject]] for privilege check
   * @param outputActionType [[PrivilegeObjectActionType]] for output [[PrivilegeObject]]
   */
  case class CmdPrivilegeBuilder(
      operationType: OperationType = QUERY,
      leastVer: Option[String] = None,
      mostVer: Option[String] = None,
      commandTypes: Seq[CommandType] = Seq.empty,
      buildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[CommandType]) => Unit =
        v2Commands.defaultBuildInput,
      buildOutput: (
          LogicalPlan,
          ArrayBuffer[PrivilegeObject],
          Seq[CommandType],
          PrivilegeObjectActionType) => Unit = v2Commands.defaultBuildOutput,
      outputActionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER)
    extends super.Val {

    def buildPrivileges(
        plan: LogicalPlan,
        inputObjs: ArrayBuffer[PrivilegeObject],
        outputObjs: ArrayBuffer[PrivilegeObject]): Unit = {
      this.buildInput(plan, inputObjs, commandTypes)
      this.buildOutput(plan, outputObjs, commandTypes, outputActionType)
    }
  }

  // dml commands

  val DeleteFromIcebergTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_DROPPARTS,
    leastVer = Some("3.2"),
    commandTypes = Seq(v2Commands.CommandType.HasTableAsIdentifierOption),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val UpdateIcebergTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    operationType = ALTERTABLE_ADDPARTS,
    leastVer = Some("3.2"),
    commandTypes = Seq(v2Commands.CommandType.HasTableAsIdentifierOption),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val UnresolvedMergeIntoIcebergTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder()

  val MergeIntoIcebergTable: CmdPrivilegeBuilder = CmdPrivilegeBuilder(
    buildInput = v2Commands.MergeIntoTable.buildInput,
    buildOutput = v2Commands.MergeIntoTable.buildOutput)
}
