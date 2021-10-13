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

package org.apache.kyuubi.sql.zorder

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Ascending, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Repartition, RepartitionByExpression, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}

import org.apache.kyuubi.sql.KyuubiSQLConf

/**
 * TODO: shall we forbid zorder if it's dynamic partition inserts ?
 * Insert zorder before writing datasource if the target table properties has zorder properties
 */
case class InsertZorderBeforeWritingDatasource(session: SparkSession)
  extends InsertZorderHelper {
  override def applyInternal(plan: LogicalPlan): LogicalPlan = plan match {
    case insert: InsertIntoHadoopFsRelationCommand if insert.query.resolved &&
      insert.bucketSpec.isEmpty && insert.catalogTable.isDefined &&
          isZorderEnabled(insert.catalogTable.get.properties) =>
      val newQuery = insertZorder(insert.catalogTable.get, insert.query)
      if (newQuery.eq(insert.query)) {
        insert
      } else {
        insert.copy(query = newQuery)
      }

    case ctas: CreateDataSourceTableAsSelectCommand if ctas.query.resolved &&
      ctas.table.bucketSpec.isEmpty && isZorderEnabled(ctas.table.properties) =>
      val newQuery = insertZorder(ctas.table, ctas.query)
      if (newQuery.eq(ctas.query)) {
        ctas
      } else {
        ctas.copy(query = newQuery)
      }

    case _ => plan
  }
}

/**
 * TODO: shall we forbid zorder if it's dynamic partition inserts ?
 * Insert zorder before writing hive if the target table properties has zorder properties
 */
case class InsertZorderBeforeWritingHive(session: SparkSession)
  extends InsertZorderHelper {
  override def applyInternal(plan: LogicalPlan): LogicalPlan = plan match {
    case insert: InsertIntoHiveTable if insert.query.resolved &&
      insert.table.bucketSpec.isEmpty && isZorderEnabled(insert.table.properties) =>
      val newQuery = insertZorder(insert.table, insert.query)
      if (newQuery.eq(insert.query)) {
        insert
      } else {
        insert.copy(query = newQuery)
      }

    case ctas: CreateHiveTableAsSelectCommand if ctas.query.resolved &&
      ctas.tableDesc.bucketSpec.isEmpty && isZorderEnabled(ctas.tableDesc.properties) =>
      val newQuery = insertZorder(ctas.tableDesc, ctas.query)
      if (newQuery.eq(ctas.query)) {
        ctas
      } else {
        ctas.copy(query = newQuery)
      }

    case octas: OptimizedCreateHiveTableAsSelectCommand if octas.query.resolved &&
      octas.tableDesc.bucketSpec.isEmpty && isZorderEnabled(octas.tableDesc.properties) =>
      val newQuery = insertZorder(octas.tableDesc, octas.query)
      if (newQuery.eq(octas.query)) {
        octas
      } else {
        octas.copy(query = newQuery)
      }

    case _ => plan
  }
}

trait InsertZorderHelper extends Rule[LogicalPlan] {
  private val KYUUBI_ZORDER_ENABLED = "kyuubi.zorder.enabled"
  private val KYUUBI_ZORDER_COLS = "kyuubi.zorder.cols"

  def isZorderEnabled(props: Map[String, String]): Boolean = {
    props.contains(KYUUBI_ZORDER_ENABLED) &&
      "true".equalsIgnoreCase(props(KYUUBI_ZORDER_ENABLED)) &&
      props.contains(KYUUBI_ZORDER_COLS)
  }

  def getZorderColumns(props: Map[String, String]): Seq[String] = {
    val cols = props.get(KYUUBI_ZORDER_COLS)
    assert(cols.isDefined)
    cols.get.split(",").map(_.trim.toLowerCase(Locale.ROOT))
  }

  def canInsertZorder(query: LogicalPlan): Boolean = query match {
    case Project(_, child) => canInsertZorder(child)
    // TODO: actually, we can force zorder even if existed some shuffle
    case _: Sort => false
    case _: RepartitionByExpression => false
    case _: Repartition => false
    case _ => true
  }

  def insertZorder(catalogTable: CatalogTable, plan: LogicalPlan): LogicalPlan = {
    if (!canInsertZorder(plan)) {
      return plan
    }
    val cols = getZorderColumns(catalogTable.properties)
    val attrs = plan.output.map(attr => (attr.name, attr)).toMap
    if (cols.exists(!attrs.contains(_))) {
      logWarning(s"target table does not contain all zorder cols: ${cols.mkString(",")}, " +
        s"please check your table properties ${KYUUBI_ZORDER_COLS}.")
      plan
    } else {
      val bound = cols.map(attrs(_))
      val orderExpr = if (bound.length == 1) {
        bound.head
      } else {
        Zorder(bound)
      }
      Sort(
        SortOrder(orderExpr, Ascending, NullsLast, Seq.empty) :: Nil,
        true,
        plan
      )
    }
  }

  def applyInternal(plan: LogicalPlan): LogicalPlan

  final override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(KyuubiSQLConf.INSERT_ZORDER_BEFORE_WRITING)) {
      applyInternal(plan)
    } else {
      plan
    }
  }
}
