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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.ExplainCommand
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._
import org.apache.kyuubi.plugin.spark.authz.rule.Authorization._
import org.apache.kyuubi.plugin.spark.authz.rule.rowfilter._
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.util.reflect.ReflectUtils._

object PrivilegesBuilder {

  final private val LOG = LoggerFactory.getLogger(getClass)

  private def collectLeaves(expr: Expression): Seq[NamedExpression] = {
    expr.collect { case p: NamedExpression if p.children.isEmpty => p }
  }

  private def setCurrentDBIfNecessary(
      tableIdent: Table,
      spark: SparkSession): Table = {
    if (tableIdent.database.isEmpty) {
      tableIdent.copy(database = Some(spark.catalog.currentDatabase))
    } else {
      tableIdent
    }
  }

  /**
   * Build PrivilegeObjects from Spark LogicalPlan
   *
   * @param plan a Spark LogicalPlan used to generate SparkPrivilegeObjects
   * @param privilegeObjects input or output spark privilege object list
   * @param projectionList Projection list after pruning
   */
  def buildQuery(
      plan: LogicalPlan,
      privilegeObjects: ArrayBuffer[PrivilegeObject],
      projectionList: Seq[NamedExpression] = Nil,
      conditionList: Seq[NamedExpression] = Nil,
      spark: SparkSession): Unit = {

    def mergeProjection(table: Table, plan: LogicalPlan): Unit = {
      if (projectionList.isEmpty) {
        privilegeObjects += PrivilegeObject(table, plan.output.map(_.name))
      } else {
        val cols = columnPrune(projectionList, plan.outputSet)
        privilegeObjects += PrivilegeObject(table, cols.map(_.name).distinct)
      }
    }

    def columnPrune(projectionList: Seq[Expression], output: AttributeSet): Seq[NamedExpression] = {
      (projectionList ++ conditionList)
        .flatMap(collectLeaves)
        .filter(output.contains)
    }

    plan match {
      case p if p.getTagValue(KYUUBI_AUTHZ_TAG).nonEmpty =>

      case scan if isKnownScan(scan) && scan.resolved =>
        val tables = getScanSpec(scan).tables(scan, spark)
        // If the the scan is table-based, we check privileges on the table we found
        // otherwise, we check privileges on the uri we found
        if (tables.nonEmpty) {
          tables.foreach(mergeProjection(_, scan))
        } else {
          getScanSpec(scan).uris(scan).foreach(privilegeObjects += PrivilegeObject(_))
        }

      case u if u.nodeName == "UnresolvedRelation" =>
        val parts = invokeAs[String](u, "tableName").split("\\.")
        val db = quote(parts.init)
        val table = Table(None, Some(db), parts.last, None)
        privilegeObjects += PrivilegeObject(table)

      case p =>
        for (child <- p.children) {
          // If current plan's references don't have relation to it's input, have two cases
          //   1. `MapInPandas`, `ScriptTransformation`
          //   2. `Project` output only have constant value
          if (columnPrune(p.references.toSeq ++ p.output, p.inputSet).isEmpty) {
            // If plan is project and output don't have relation to input, can ignore.
            if (!p.isInstanceOf[Project]) {
              buildQuery(
                child,
                privilegeObjects,
                p.inputSet.map(_.toAttribute).toSeq,
                Nil,
                spark)
            }
          } else {
            buildQuery(
              child,
              privilegeObjects,
              // Here we use `projectList ++ p.reference` do column prune.
              // For `Project`, `Aggregate`, plan's output is contained by plan's referenced
              // For `Filter`, `Sort` etc... it rely on upper `Project` node,
              //    since we wrap a `Project` before call `buildQuery()`.
              // So here we use upper node's projectionList and current's references
              // to do column pruning can get the correct column.
              columnPrune(projectionList ++ p.references.toSeq, p.inputSet).distinct,
              conditionList ++ p.references,
              spark)
          }
        }
    }
  }

  /**
   * Build PrivilegeObjects from Spark LogicalPlan
   * @param plan a Spark LogicalPlan used to generate Spark PrivilegeObjects
   * @param inputObjs input privilege object list
   * @param outputObjs output privilege object list
   */
  private def buildCommand(
      plan: LogicalPlan,
      inputObjs: ArrayBuffer[PrivilegeObject],
      outputObjs: ArrayBuffer[PrivilegeObject],
      spark: SparkSession): OperationType = {

    def getTablePriv(tableDesc: TableDesc): Seq[PrivilegeObject] = {
      try {
        val maybeTable = tableDesc.extract(plan, spark)
        maybeTable match {
          case Some(table) =>
            val newTable = if (tableDesc.setCurrentDatabaseIfMissing) {
              setCurrentDBIfNecessary(table, spark)
            } else {
              table
            }
            if (tableDesc.tableTypeDesc.exists(_.skip(plan))) {
              Nil
            } else {
              val actionType = tableDesc.actionTypeDesc.map(_.extract(plan)).getOrElse(OTHER)
              val columnNames = tableDesc.columnDesc.map(_.extract(plan)).getOrElse(Nil)
              Seq(PrivilegeObject(newTable, columnNames, actionType))
            }
          case None => Nil
        }
      } catch {
        case e: Exception =>
          LOG.debug(tableDesc.error(plan, e))
          Nil
      }
    }

    plan.getClass.getName match {
      case classname if DB_COMMAND_SPECS.contains(classname) =>
        val desc = DB_COMMAND_SPECS(classname)
        desc.databaseDescs.foreach { databaseDesc =>
          try {
            val database = databaseDesc.extract(plan)
            if (databaseDesc.isInput) {
              inputObjs += PrivilegeObject(database)
            } else {
              outputObjs += PrivilegeObject(database)
            }
          } catch {
            case e: Exception =>
              LOG.debug(databaseDesc.error(plan, e))
          }
        }
        desc.uriDescs.foreach { ud =>
          try {
            val uris = ud.extract(plan, spark)
            if (ud.isInput) {
              inputObjs ++= uris.map(PrivilegeObject(_))
            } else {
              outputObjs ++= uris.map(PrivilegeObject(_))
            }
          } catch {
            case e: Exception =>
              LOG.debug(ud.error(plan, e))
          }
        }
        desc.operationType

      case classname if TABLE_COMMAND_SPECS.contains(classname) =>
        val spec = TABLE_COMMAND_SPECS(classname)
        spec.tableDescs.foreach { td =>
          if (td.isInput) {
            inputObjs ++= getTablePriv(td)
          } else {
            outputObjs ++= getTablePriv(td)
          }
        }
        spec.uriDescs.foreach { ud =>
          try {
            val uris = ud.extract(plan, spark)
            if (ud.isInput) {
              inputObjs ++= uris.map(PrivilegeObject(_))
            } else {
              outputObjs ++= uris.map(PrivilegeObject(_))
            }
          } catch {
            case e: Exception =>
              LOG.debug(ud.error(plan, e))
          }
        }
        spec.queries(plan).foreach { p =>
          buildQuery(Project(p.output, p), inputObjs, spark = spark)
        }
        spec.operationType

      case classname if FUNCTION_COMMAND_SPECS.contains(classname) =>
        val spec = FUNCTION_COMMAND_SPECS(classname)
        spec.functionDescs.foreach { fd =>
          try {
            val function = fd.extract(plan)
            if (!fd.functionTypeDesc.exists(_.skip(plan, spark))) {
              if (fd.isInput) {
                inputObjs += PrivilegeObject(function)
              } else {
                outputObjs += PrivilegeObject(function)
              }
            }
          } catch {
            case e: Exception =>
              LOG.debug(fd.error(plan, e))
          }
        }
        spec.operationType

      case _ => OperationType.QUERY
    }
  }

  type PrivilegesAndOpType = (Iterable[PrivilegeObject], Iterable[PrivilegeObject], OperationType)

  /**
   * Build input  privilege objects from a Spark's LogicalPlan for hive permanent udf
   *
   * @param plan      A Spark LogicalPlan
   */
  def buildFunctions(
      plan: LogicalPlan,
      spark: SparkSession): PrivilegesAndOpType = {
    val inputObjs = new ArrayBuffer[PrivilegeObject]
    plan match {
      case command: Command if isKnownTableCommand(command) =>
        val spec = getTableCommandSpec(command)
        val functionPrivAndOpType = spec.queries(plan)
          .map(plan => buildFunctions(plan, spark))
        inputObjs ++= functionPrivAndOpType.flatMap(_._1)

      case plan => plan transformAllExpressions {
          case hiveFunction: Expression if isKnownFunction(hiveFunction) =>
            val functionSpec: ScanSpec = getFunctionSpec(hiveFunction)
            if (functionSpec.functionDescs
                .exists(!_.functionTypeDesc.get.skip(hiveFunction, spark))) {
              functionSpec.functions(hiveFunction).foreach(func =>
                inputObjs += PrivilegeObject(func))
            }
            hiveFunction
        }
    }
    (inputObjs, Seq.empty, OperationType.QUERY)
  }

  /**
   * Build input and output privilege objects from a Spark's LogicalPlan
   *
   * For `Command`s, build outputs if it has an target to write, build inputs for the
   * inside query if exists.
   *
   * For other queries, build inputs.
   *
   * @param plan A Spark LogicalPlan
   */
  def build(
      plan: LogicalPlan,
      spark: SparkSession): PrivilegesAndOpType = {
    val inputObjs = new ArrayBuffer[PrivilegeObject]
    val outputObjs = new ArrayBuffer[PrivilegeObject]
    val opType = plan match {
      case ObjectFilterPlaceHolder(child) if child.nodeName == "ShowTables" =>
        OperationType.SHOWTABLES
      case ObjectFilterPlaceHolder(child) if child.nodeName == "ShowNamespaces" =>
        OperationType.SHOWDATABASES
      case _: FilteredShowTablesCommand => OperationType.SHOWTABLES
      case _: FilteredShowFunctionsCommand => OperationType.SHOWFUNCTIONS
      case _: FilteredShowColumnsCommand => OperationType.SHOWCOLUMNS

      // ExplainCommand run will execute the plan, should avoid check privilege for the plan.
      case _: ExplainCommand =>
        setExplainCommandExecutionId(spark)
        OperationType.EXPLAIN
      case _ if isExplainCommandChild(spark) =>
        OperationType.EXPLAIN
      // RunnableCommand
      case cmd: Command => buildCommand(cmd, inputObjs, outputObjs, spark)
      // Queries
      case _ =>
        buildQuery(Project(plan.output, plan), inputObjs, spark = spark)
        OperationType.QUERY
    }
    (inputObjs, outputObjs, opType)
  }
}
