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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Identifier
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType._
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.PermanentViewMarker

object PrivilegesBuilder {

  final private val LOG = LoggerFactory.getLogger(getClass)

  def databasePrivileges(db: String): PrivilegeObject = {
    PrivilegeObject(DATABASE, PrivilegeObjectActionType.OTHER, db, db)
  }

  private def tablePrivileges(
      table: TableIdentifier,
      columns: Seq[String] = Nil,
      owner: Option[String] = None,
      actionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER): PrivilegeObject = {
    PrivilegeObject(TABLE_OR_VIEW, actionType, table.database.orNull, table.table, columns, owner)
  }

  private def v2TablePrivileges(
      table: Identifier,
      columns: Seq[String] = Nil,
      owner: Option[String] = None,
      actionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER): PrivilegeObject = {
    PrivilegeObject(
      TABLE_OR_VIEW,
      actionType,
      quote(table.namespace()),
      table.name(),
      columns,
      owner)
  }
  private def functionPrivileges(
      function: Function): PrivilegeObject = {
    PrivilegeObject(
      FUNCTION,
      PrivilegeObjectActionType.OTHER,
      function.database.orNull,
      function.functionName)
  }

  private def collectLeaves(expr: Expression): Seq[NamedExpression] = {
    expr.collect { case p: NamedExpression if p.children.isEmpty => p }
  }

  private def setCurrentDBIfNecessary(
      tableIdent: TableIdentifier,
      spark: SparkSession): TableIdentifier = {

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
      conditionList: Seq[NamedExpression] = Nil): Unit = {

    def mergeProjection(table: CatalogTable, plan: LogicalPlan): Unit = {
      val tableOwner = extractTableOwner(table)
      if (projectionList.isEmpty) {
        privilegeObjects += tablePrivileges(
          table.identifier,
          table.schema.fieldNames,
          tableOwner)
      } else {
        val cols = (projectionList ++ conditionList).flatMap(collectLeaves)
          .filter(plan.outputSet.contains).map(_.name).distinct
        privilegeObjects += tablePrivileges(table.identifier, cols, tableOwner)
      }
    }

    def mergeProjectionV2Table(
        table: Identifier,
        plan: LogicalPlan,
        owner: Option[String] = None): Unit = {
      if (projectionList.isEmpty) {
        privilegeObjects += v2TablePrivileges(table, plan.output.map(_.name), owner)
      } else {
        val cols = (projectionList ++ conditionList).flatMap(collectLeaves)
          .filter(plan.outputSet.contains).map(_.name).distinct
        privilegeObjects += v2TablePrivileges(table, cols, owner)
      }
    }

    plan match {
      case p: Project => buildQuery(p.child, privilegeObjects, p.projectList, conditionList)

      case j: Join =>
        val cols =
          conditionList ++ j.condition.map(expr => collectLeaves(expr)).getOrElse(Nil)
        buildQuery(j.left, privilegeObjects, projectionList, cols)
        buildQuery(j.right, privilegeObjects, projectionList, cols)

      case f: Filter =>
        val cols = conditionList ++ collectLeaves(f.condition)
        buildQuery(f.child, privilegeObjects, projectionList, cols)

      case w: Window =>
        val orderCols = w.orderSpec.flatMap(orderSpec => collectLeaves(orderSpec))
        val partitionCols = w.partitionSpec.flatMap(partitionSpec => collectLeaves(partitionSpec))
        val cols = conditionList ++ orderCols ++ partitionCols
        buildQuery(w.child, privilegeObjects, projectionList, cols)

      case s: Sort =>
        val sortCols = s.order.flatMap(sortOrder => collectLeaves(sortOrder))
        val cols = conditionList ++ sortCols
        buildQuery(s.child, privilegeObjects, projectionList, cols)

      case hiveTableRelation if hasResolvedHiveTable(hiveTableRelation) =>
        mergeProjection(getHiveTable(hiveTableRelation), hiveTableRelation)

      case logicalRelation if hasResolvedDatasourceTable(logicalRelation) =>
        getDatasourceTable(logicalRelation).foreach { t =>
          mergeProjection(t, plan)
        }

      case datasourceV2Relation if hasResolvedDatasourceV2Table(datasourceV2Relation) =>
        val tableIdent = getDatasourceV2Identifier(datasourceV2Relation)
        if (tableIdent.isDefined) {
          mergeProjectionV2Table(
            tableIdent.get,
            plan,
            getDatasourceV2TableOwner(datasourceV2Relation))
        }

      case u if u.nodeName == "UnresolvedRelation" =>
        val parts = invokeAs[String](u, "tableName").split("\\.")
        val db = quote(parts.init)
        privilegeObjects += tablePrivileges(TableIdentifier(parts.last, Some(db)))

      case permanentViewMarker: PermanentViewMarker =>
        mergeProjection(permanentViewMarker.catalogTable, plan)

      case p =>
        for (child <- p.children) {
          buildQuery(child, privilegeObjects, projectionList, conditionList)
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
            // TODO Use strings instead of ti for general purpose.
            var identifier = TableIdentifier(table.table, table.database)
            if (tableDesc.setCurrentDatabaseIfMissing) {
              identifier = setCurrentDBIfNecessary(identifier, spark)
            }
            if (tableDesc.tableTypeDesc.exists(_.skip(plan))) {
              Nil
            } else {
              val actionType = tableDesc.actionTypeDesc.map(_.extract(plan)).getOrElse(OTHER)
              val columnNames = tableDesc.columnDesc.map(_.extract(plan)).getOrElse(Nil)
              Seq(tablePrivileges(identifier, columnNames, table.owner, actionType))
            }
          case None => Nil
        }
      } catch {
        case e: Exception =>
          LOG.warn(tableDesc.error(plan, e))
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
              inputObjs += databasePrivileges(database)
            } else {
              outputObjs += databasePrivileges(database)
            }
          } catch {
            case e: Exception =>
              LOG.warn(databaseDesc.error(plan, e))
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
        spec.queryDescs.foreach { qd =>
          try {
            buildQuery(qd.extract(plan), inputObjs)
          } catch {
            case e: Exception =>
              LOG.warn(qd.error(plan, e))
          }
        }
        spec.operationType

      case classname if FUNCTION_COMMAND_SPECS.contains(classname) =>
        val spec = FUNCTION_COMMAND_SPECS(classname)
        spec.functionDescs.foreach { fd =>
          try {
            val function = fd.extract(plan)
            if (!fd.functionTypeDesc.exists(_.skip(plan, spark))) {
              if (fd.isInput) {
                inputObjs += functionPrivileges(function)
              } else {
                outputObjs += functionPrivileges(function)
              }
            }
          } catch {
            case e: Exception =>
              LOG.warn(fd.error(plan, e))
          }
        }
        spec.operationType

      case _ => OperationType.QUERY
    }
  }

  type PrivilegesAndOpType = (Seq[PrivilegeObject], Seq[PrivilegeObject], OperationType)

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
      // RunnableCommand
      case cmd: Command => buildCommand(cmd, inputObjs, outputObjs, spark)
      // Queries
      case _ =>
        buildQuery(plan, inputObjs)
        OperationType.QUERY
    }
    (inputObjs, outputObjs, opType)
  }
}
