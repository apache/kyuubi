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
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType.PrivilegeObjectActionType
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType.TABLE_OR_VIEW
import org.apache.kyuubi.plugin.spark.authz.PrivilegesBuilder._
import org.apache.kyuubi.plugin.spark.authz.V2CommandType.{HasQuery, V2AlterTableCommand, V2CommandType, V2CreateTablePlan, V2DdlTableCommand, V2PartitionCommand, V2WriteCommand}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

object V2CommandType extends Enumeration {
  type V2CommandType = Value

  // traits' name from Spark v2commands and v2AlterTableCommands
  val V2CreateTablePlan, V2WriteCommand, V2AlterTableCommand, V2PartitionCommand = Value

  // custom types
  val HasQuery, V2DdlTableCommand = Value
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
        (cmd.mostVer.isEmpty || isSparkVersionAtMost(cmd.mostVer.get)) &&
          (cmd.leastVer.isEmpty || isSparkVersionAtLeast(cmd.leastVer.get))

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

        case V2AlterTableCommand if isSparkVersionAtLeast("3.2") =>
          val table = getFieldVal[LogicalPlan](p, "table")
          val tableIdent = getFieldVal[Identifier](table, "identifier")
          outputObjs += v2TablePrivileges(tableIdent)

        case V2DdlTableCommand =>
          val table = getFieldVal[AnyRef](p, "child")
          val tableIdent = getFieldVal[Identifier](table, "identifier")
          outputObjs += v2TablePrivileges(tableIdent)

        case V2PartitionCommand =>
          val table = getFieldVal[AnyRef](p, "table")
          val tableIdent = getFieldVal[Identifier](table, "identifier")
          outputObjs += v2TablePrivileges(tableIdent)

        case _ =>
      }
    }

  case class V2Command(
      operationType: OperationType = QUERY,
      leastVer: Option[String] = None,
      mostVer: Option[String] = None,
      cmdTypes: Seq[V2CommandType] = Seq(),
      buildInput: (LogicalPlan, ArrayBuffer[PrivilegeObject], Seq[V2CommandType]) => Unit =
        defaultBuildInput,
      buildOutput: (
          LogicalPlan,
          ArrayBuffer[PrivilegeObject],
          Seq[V2CommandType],
          PrivilegeObjectActionType) => Unit = defaultBuildOutput,
      outputActionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER,
      enabled: Boolean = true) {

    def buildCommand(
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
    operationType = CREATEDATABASE,
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
    operationType = DROPDATABASE,
    buildOutput = (plan, outputObjs, _, _) => {
      val resolvedNamespace = getFieldVal[LogicalPlan](plan, "namespace")
      val databases = getFieldVal[Seq[String]](resolvedNamespace, "namespace")
      outputObjs += databasePrivileges(quote(databases))
    })

  // with V2CreateTablePlan

  val CreateTable: V2Command = V2Command(
    operationType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan),
    leastVer = Some("3.3"))

  val CreateV2Table: V2Command = V2Command(
    operationType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan),
    mostVer = Some("3.2"))

  val CreateTableAsSelect: V2Command = V2Command(
    operationType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan, HasQuery))

  val ReplaceTable: V2Command = V2Command(
    operationType = CREATETABLE,
    cmdTypes = Seq(V2CreateTablePlan))

  val ReplaceTableAsSelect: V2Command = V2Command(
    operationType = CREATETABLE,
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

  // with V2PartitionCommand

  val AddPartitions: V2Command = V2Command(
    operationType = OperationType.ALTERTABLE_ADDPARTS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2PartitionCommand))

  val DropPartitions: V2Command = V2Command(
    operationType = OperationType.ALTERTABLE_DROPPARTS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2PartitionCommand))

  val RenamePartitions: V2Command = V2Command(
    operationType = OperationType.ALTERTABLE_ADDPARTS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2PartitionCommand))

  val TruncatePartition: V2Command = V2Command(
    operationType = OperationType.ALTERTABLE_DROPPARTS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2PartitionCommand))

  // other table commands

  val DropTable: V2Command = V2Command(
    operationType = DROPTABLE,
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
    operationType = CREATEVIEW,
    leastVer = Some("3.2"),
    buildInput = (plan, inputObjs, _) => {
      val query = getFieldVal[LogicalPlan](plan, "table") // table to cache
      buildQuery(query, inputObjs)
    })

  val CacheTableAsSelect: V2Command = V2Command(
    operationType = CREATEVIEW,
    leastVer = Some("3.2"),
    buildInput = (plan, inputObjs, _) => {
      val query = getFieldVal[LogicalPlan](plan, "plan")
      buildQuery(query, inputObjs)
    })

  val CommentOnNamespace: V2Command = V2Command(
    operationType = ALTERDATABASE,
    buildOutput = (plan, outputObjs, _, _) => {
      val resolvedNamespace = getFieldVal[AnyRef](plan, "child")
      val namespace = getFieldVal[Seq[String]](resolvedNamespace, "namespace")
      outputObjs += databasePrivileges(quote(namespace))
    })

  val CommentOnTable: V2Command = V2Command(
    operationType = ALTERTABLE_PROPERTIES,
    cmdTypes = Seq(
      if (isSparkVersionAtLeast("3.2")) V2AlterTableCommand else V2DdlTableCommand))

  val MergeIntoTable: V2Command = V2Command(
    buildInput = (plan, inputObjs, _) => {
      val table = getFieldVal[DataSourceV2Relation](plan, "sourceTable")
      buildQuery(table, inputObjs)
    },
    buildOutput = (plan, outputObjs, _, _) => {
      val table = getFieldVal[DataSourceV2Relation](plan, "targetTable")
      if (table.identifier.isDefined) {
        outputObjs += v2TablePrivileges(
          table.identifier.get,
          actionType = PrivilegeObjectActionType.UPDATE)
      }
    })

  val RepairTable: V2Command = V2Command(
    operationType = ALTERTABLE_ADDPARTS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2DdlTableCommand))

  val TruncateTable: V2Command = V2Command(
    leastVer = Some("3.2"),
    buildOutput = (plan, outputObjs, _, _) => {
      val table = getFieldVal[Any](plan, "table")
      val tableIdent = getFieldVal[Identifier](table, "identifier")
      outputObjs += v2TablePrivileges(tableIdent, actionType = PrivilegeObjectActionType.UPDATE)
    })

  // with V2AlterTableCommand

  val AlterTable: V2Command = V2Command(
    operationType = ALTERTABLE_ADDCOLS,
    mostVer = Some("3.1"),
    buildOutput = (plan, outputObjs, _, _) => {
      val table = getFieldVal[Any](plan, "table")
      val tableIdent = getFieldVal[Option[Identifier]](table, "identifier")
      if (tableIdent.isDefined) {
        outputObjs += v2TablePrivileges(tableIdent.get)
      }
    })

  val AddColumns: V2Command = V2Command(
    operationType = ALTERTABLE_ADDCOLS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2AlterTableCommand))

  val AlterColumn: V2Command = V2Command(
    operationType = ALTERTABLE_ADDCOLS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2AlterTableCommand))

  val DropColumns: V2Command = V2Command(
    operationType = ALTERTABLE_ADDCOLS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2AlterTableCommand))

  val ReplaceColumns: V2Command = V2Command(
    operationType = ALTERTABLE_REPLACECOLS,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2AlterTableCommand))

  val RenameColumn: V2Command = V2Command(
    operationType = ALTERTABLE_RENAMECOL,
    leastVer = Some("3.2"),
    cmdTypes = Seq(V2AlterTableCommand))
}
