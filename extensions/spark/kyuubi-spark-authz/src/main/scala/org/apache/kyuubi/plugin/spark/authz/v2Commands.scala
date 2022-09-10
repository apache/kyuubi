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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier

import org.apache.kyuubi.plugin.spark.authz.OperationType.{ALTERTABLE_ADDCOLS, ALTERTABLE_RENAMECOL, ALTERTABLE_REPLACECOLS, CREATEDATABASE, CREATETABLE, CREATEVIEW, DROPDATABASE, DROPTABLE, OperationType, QUERY}
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType.TABLE_OR_VIEW
import org.apache.kyuubi.plugin.spark.authz.PrivilegesBuilder._
import org.apache.kyuubi.plugin.spark.authz.V2CommandType.{HasQuery, V2AlterTableCommand, V2CommandType, V2CreateTablePlan, V2WriteCommand}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getFieldVal, invoke, isSparkVersionAtLeast, isSparkVersionAtMost, quote}

object V2CommandType extends Enumeration {
  type V2CommandType = Value

  // traits' name from Spark v2commands and v2AlterTableCommands
  val V2CreateTablePlan, V2WriteCommand, V2AlterTableCommand = Value

  // with query plan
  val HasQuery = Value
}

object v2Commands extends Enumeration {

  import scala.language.implicitConversions

  implicit def valueToV2CommandBuilder(x: Value): V2Command =
    x.asInstanceOf[V2Command]

  def accept(commandName: String): Boolean = {
    try {
      val cmd = v2Commands.withName(commandName)

      // check spark version requirements
      def passSparkVersionCheck: Boolean =
        (StringUtils.isBlank(cmd.mostVer) || isSparkVersionAtMost(cmd.mostVer)) &&
          (StringUtils.isBlank(cmd.leastVer) || isSparkVersionAtLeast(cmd.leastVer))

      passSparkVersionCheck && cmd.enabled
    } catch {
      case _: NoSuchElementException => false
    }
  }

  val defaultBuildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[V2CommandType]) => Unit =
    (p, inputObjs, cmdTypes) => {
      cmdTypes.foreach {
        case HasQuery =>
          val query = getFieldVal[LogicalPlan](p, "query")
          buildQuery(query, inputObjs)
        case _ =>
      }
    }

  val defaultBuildOutput: (
      LogicalPlan,
      ArrayBuffer[PrivilegeObject],
      Seq[V2CommandType],
      PrivilegeObjectActionType) => Unit =
    (p, outputObjs, cmdTypes, outputObjsActionType) => {
      cmdTypes.foreach {
        case V2CreateTablePlan =>
          val table = invoke(p, "tableName").asInstanceOf[Identifier]
          outputObjs += v2TablePrivileges(table)

        case V2WriteCommand =>
          val table = getFieldVal[AnyRef](p, "table")
          val tableIdent = getFieldVal[Option[Identifier]](table, "identifier")
          if (tableIdent.isDefined) {
            outputObjs += v2TablePrivileges(tableIdent.get, actionType = outputObjsActionType)
          }

        case V2AlterTableCommand =>
          val table = getFieldVal[LogicalPlan](p, "table")
          val tableIdent = getFieldVal[Identifier](table, "identifier")
          outputObjs += v2TablePrivileges(tableIdent)

        case _ =>
      }
    }

  case class V2Command(
      leastVer: String = "",
      mostVer: String = "",
      buildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[V2CommandType]) => Unit =
        defaultBuildInput,
      buildOutput: (
          LogicalPlan,
          ArrayBuffer[PrivilegeObject],
          Seq[V2CommandType],
          PrivilegeObjectActionType) => Unit = defaultBuildOutput,
      operType: OperationType = QUERY,
      cmdTypes: Seq[V2CommandType] = Seq(),
      outputActionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER,
      enabled: Boolean = true)
    extends super.Val {

    def handle(
        plan: LogicalPlan,
        inputObjs: ArrayBuffer[PrivilegeObject],
        outputObjs: ArrayBuffer[PrivilegeObject]): Unit = {
      this.buildInput(plan, inputObjs, cmdTypes)
      this.buildOutput(plan, outputObjs, cmdTypes, outputActionType)
    }
  }

  def v2TablePrivileges(
      table: Identifier,
      columns: Seq[String] = Nil,
      actionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER): PrivilegeObject = {
    PrivilegeObject(TABLE_OR_VIEW, actionType, quote(table.namespace()), table.name(), columns)
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // commands

  val CreateNamespace: V2Command = V2Command(
    operType = CREATEDATABASE,
    buildOutput = (plan, outputObjs, _, _) => {
      if (isSparkVersionAtLeast("3.3")) {
        val resolvedNamespace = getFieldVal[Any](plan, "name")
        val databases = getFieldVal[Seq[String]](resolvedNamespace, "nameParts")
        outputObjs += databasePrivileges(quote(databases))
      } else {
        val namespace = getFieldVal[Seq[String]](plan, "namespace")
        outputObjs += databasePrivileges(quote(namespace))
      }
    })

  val DropNamespace: V2Command = V2Command(
    operType = DROPDATABASE,
    buildOutput = (plan, outputObjs, _, _) => {
      val resolvedNamespace = getFieldVal[LogicalPlan](plan, "namespace")
      val databases = getFieldVal[Seq[String]](resolvedNamespace, "namespace")
      outputObjs += databasePrivileges(quote(databases))
    })

  // with V2CreateTablePlan

  val CreateTable: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan),
    leastVer = "3.3")

  val CreateV2Table: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan),
    mostVer = "3.2")

  val CreateTableAsSelect: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan, HasQuery))

  val ReplaceTable: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan))

  val ReplaceTableAsSelect: V2Command = V2Command(
    operType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan, HasQuery))

  // with V2WriteCommand
  val AppendData: V2Command = V2Command(
    cmdTypes = Seq(V2WriteCommand, HasQuery),
    outputActionType = PrivilegeObjectActionType.INSERT)

  val UpdateTable: V2Command = V2Command(
    cmdTypes = Seq(V2WriteCommand),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val DeleteFromTable: V2Command = V2Command(
    cmdTypes = Seq(V2WriteCommand),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val OverwriteByExpression: V2Command = V2Command(
    cmdTypes = Seq(V2WriteCommand, HasQuery),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  val OverwritePartitionsDynamic: V2Command = V2Command(
    cmdTypes = Seq(V2WriteCommand, HasQuery),
    outputActionType = PrivilegeObjectActionType.UPDATE)

  // other table commands
  val DropTable: V2Command = V2Command(
    operType = DROPTABLE,
    buildOutput = (plan, outputObjs, _, _) => {
      val tableIdent =
        if (isSparkVersionAtLeast("3.1")) {
          val resolvedTable = getFieldVal[LogicalPlan](plan, "child")
          getFieldVal[Identifier](resolvedTable, "identifier")
        } else {
          getFieldVal[Identifier](plan, "ident")
        }
      outputObjs += v2TablePrivileges(tableIdent)
    })

  val CacheTable: V2Command = V2Command(
    operType = CREATEVIEW,
    leastVer = "3.2",
    buildInput = (plan, inputObjs, _) => {
      val query = getFieldVal[LogicalPlan](plan, "table") // table to cache
      buildQuery(query, inputObjs)
    })

  val CacheTableAsSelect: V2Command = V2Command(
    operType = CREATEVIEW,
    leastVer = "3.2",
    buildInput = (plan, inputObjs, _) => {
      val query = getFieldVal[LogicalPlan](plan, "plan")
      buildQuery(query, inputObjs)
    })

  // v2AlterTableCommands with V2AlterTableCommand trait
  val AlterTable: V2Command = V2Command(
    operType = ALTERTABLE_ADDCOLS,
    leastVer = "3.0",
    mostVer = "3.1",
    buildOutput = (plan, outputObjs, _, _) => {
      val table = getFieldVal[Any](plan, "table")
      val tableIdent = getFieldVal[Option[Identifier]](table, "identifier")
      outputObjs += v2TablePrivileges(tableIdent.get)
    })

  val AddColumns: V2Command = V2Command(
    operType = ALTERTABLE_ADDCOLS,
    leastVer = "3.2",
    cmdTypes = Seq(V2AlterTableCommand))

  val AlterColumn: V2Command = V2Command(
    operType = ALTERTABLE_ADDCOLS,
    leastVer = "3.2",
    cmdTypes = Seq(V2AlterTableCommand))

  val DropColumns: V2Command = V2Command(
    operType = ALTERTABLE_ADDCOLS,
    leastVer = "3.2",
    cmdTypes = Seq(V2AlterTableCommand))

  val ReplaceColumns: V2Command = V2Command(
    operType = ALTERTABLE_REPLACECOLS,
    leastVer = "3.2",
    cmdTypes = Seq(V2AlterTableCommand))

  val RenameColumn: V2Command = V2Command(
    operType = ALTERTABLE_RENAMECOL,
    leastVer = "3.2",
    cmdTypes = Seq(V2AlterTableCommand))
}
