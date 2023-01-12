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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.ranger.{FilteredShowColumnsCommand, FilteredShowFunctionsCommand, FilteredShowTablesCommand, RewriteTableOwnerRunnableCommand}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getAuthzUgi, getFieldVal, setFieldVal}

trait LogicalPlanRewriter extends Rewriter {

  def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan
}

object LogicalPlanRewriter {
  val logicalPlanRewriters: Map[String, LogicalPlanRewriter] = {
    ServiceLoader.load(classOf[LogicalPlanRewriter])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }
}

class ShowTablesCommandRewriter extends LogicalPlanRewriter {

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    FilteredShowTablesCommand(plan.asInstanceOf[RunnableCommand])
  }
}

class ShowFunctionsCommandRewriter extends LogicalPlanRewriter {

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    FilteredShowFunctionsCommand(plan.asInstanceOf[RunnableCommand])
  }
}

class ShowColumnsCommandRewriter extends LogicalPlanRewriter {

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    FilteredShowColumnsCommand(plan.asInstanceOf[RunnableCommand])
  }
}

class CatalogTableCommandTableOwnerRewriter extends LogicalPlanRewriter {

  final private val LOG = LoggerFactory.getLogger(getClass)

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    val spec = TABLE_COMMAND_SPECS.get(plan.getClass.getName)
    val catalogTableOpt = spec.flatMap(
      _.tableDescs
        .filterNot(_.isInput)
        .flatMap(desc => getCatalogTable(plan, desc.fieldName).toSeq)
        .headOption)
    catalogTableOpt.foreach(catalogTable =>
      setFieldVal(catalogTable, "owner", getAuthzUgi(spark.sparkContext).getShortUserName))
    plan
  }

  def getCatalogTable(plan: LogicalPlan, field: String): Option[CatalogTable] = {
    try {
      Some(getFieldVal[CatalogTable](plan, field))
    } catch {
      case e: Exception =>
        LOG.warn(
          s"Failed to get CatalogTable from ${plan.getClass.getName} by field $field. " +
            s"Cause: ${e.toString}")
        None
    }
  }
}

class RunnableCommandTableOwnerRewriter extends LogicalPlanRewriter {

  final private val LOG = LoggerFactory.getLogger(getClass)

  protected def shouldRewrite(plan: LogicalPlan, tableExists: Boolean): Boolean = {
    !tableExists
  }

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    val spec = TABLE_COMMAND_SPECS.get(plan.getClass.getName)
    val tableOpt = spec.flatMap(
      _.tableDescs
        .filterNot(_.isInput)
        .flatMap(desc => safeExtract(desc, plan).toSeq)
        .headOption)

    tableOpt.flatMap { table =>
      val tableId = TableIdentifier(table.table, table.database)
      val catalog = spark.sessionState.catalog
      if (shouldRewrite(plan, catalog.tableExists(tableId))) {
        Some(RewriteTableOwnerRunnableCommand(plan.asInstanceOf[RunnableCommand], tableId))
      } else {
        None
      }
    }.getOrElse(plan)
  }

  private def safeExtract(tableDesc: TableDesc, plan: LogicalPlan): Option[Table] = {
    try {
      tableDesc.extract(plan)
    } catch {
      case e: Exception =>
        LOG.warn(tableDesc.error(plan, e))
        None
    }
  }
}

class CreateViewCommandTableOwnerRewriter extends RunnableCommandTableOwnerRewriter {

  override protected def shouldRewrite(plan: LogicalPlan, tableExists: Boolean): Boolean = {
    val isPermView = new ViewTypeTableTypeExtractor().apply(
      getFieldVal(plan, "viewType"),
      null) == TableType.PERMANENT_VIEW
    isPermView && (getFieldVal[Boolean](plan, "replace") || !tableExists)
  }
}
