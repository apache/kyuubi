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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import org.apache.kyuubi.sql.{KyuubiSQLConf, KyuubiSQLExtensionException}

trait ZorderBuilder {
  def buildZorder(children: Seq[Expression]): ZorderBase
}

trait InsertZorderHelper extends Rule[LogicalPlan] with ZorderBuilder {
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
    cols.get.split(",").map(_.trim)
  }

  def canInsertZorder(query: LogicalPlan): Boolean = query match {
    case Project(_, child) => canInsertZorder(child)
    case _: RepartitionOperation | _: RebalancePartitions
        if !conf.getConf(KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED) => true
    // TODO: actually, we can force zorder even if existed some shuffle
    case _: Sort => false
    case _: RepartitionOperation => false
    case _: RebalancePartitions => false
    case _ => true
  }

  def insertZorder(
      catalogTable: CatalogTable,
      plan: LogicalPlan,
      dynamicPartitionColumns: Seq[Attribute]): LogicalPlan = {
    if (!canInsertZorder(plan)) {
      return plan
    }
    val cols = getZorderColumns(catalogTable.properties)
    val resolver = session.sessionState.conf.resolver
    val output = plan.output
    val bound = cols.flatMap(col => output.find(attr => resolver(attr.name, col)))
    if (bound.size < cols.size) {
      logWarning(s"target table does not contain all zorder cols: ${cols.mkString(",")}, " +
        s"please check your table properties ${KYUUBI_ZORDER_COLS}.")
      plan
    } else {
      if (conf.getConf(KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED) &&
        conf.getConf(KyuubiSQLConf.REBALANCE_BEFORE_ZORDER)) {
        throw new KyuubiSQLExtensionException(s"${KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED.key} " +
          s"and ${KyuubiSQLConf.REBALANCE_BEFORE_ZORDER.key} can not be enabled together.")
      }
      if (conf.getConf(KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED) &&
        dynamicPartitionColumns.nonEmpty) {
        logWarning(s"Dynamic partition insertion with global sort may produce small files.")
      }

      val zorderExpr =
        if (bound.length == 1) {
          bound
        } else if (conf.getConf(KyuubiSQLConf.ZORDER_USING_ORIGINAL_ORDERING_ENABLED)) {
          bound.asInstanceOf[Seq[Expression]]
        } else {
          buildZorder(bound) :: Nil
        }
      val (global, orderExprs, child) =
        if (conf.getConf(KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED)) {
          (true, zorderExpr, plan)
        } else if (conf.getConf(KyuubiSQLConf.REBALANCE_BEFORE_ZORDER)) {
          val rebalanceExpr =
            if (dynamicPartitionColumns.isEmpty) {
              // static partition insert
              bound
            } else if (conf.getConf(KyuubiSQLConf.REBALANCE_ZORDER_COLUMNS_ENABLED)) {
              // improve data compression ratio
              dynamicPartitionColumns.asInstanceOf[Seq[Expression]] ++ bound
            } else {
              dynamicPartitionColumns.asInstanceOf[Seq[Expression]]
            }
          // for dynamic partition insert, Spark always sort the partition columns,
          // so here we sort partition columns + zorder.
          val rebalance =
            if (dynamicPartitionColumns.nonEmpty &&
              conf.getConf(KyuubiSQLConf.TWO_PHASE_REBALANCE_BEFORE_ZORDER)) {
              // improve compression ratio
              RebalancePartitions(
                rebalanceExpr,
                RebalancePartitions(dynamicPartitionColumns, plan))
            } else {
              RebalancePartitions(rebalanceExpr, plan)
            }
          (false, dynamicPartitionColumns.asInstanceOf[Seq[Expression]] ++ zorderExpr, rebalance)
        } else {
          (false, zorderExpr, plan)
        }
      val order = orderExprs.map { expr =>
        SortOrder(expr, Ascending, NullsLast, Seq.empty)
      }
      Sort(order, global, child)
    }
  }

  override def buildZorder(children: Seq[Expression]): ZorderBase = Zorder(children)

  def session: SparkSession
  def applyInternal(plan: LogicalPlan): LogicalPlan

  final override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(KyuubiSQLConf.INSERT_ZORDER_BEFORE_WRITING)) {
      applyInternal(plan)
    } else {
      plan
    }
  }
}

case class InsertZorderBeforeWritingDatasource(session: SparkSession)
  extends InsertZorderHelper {
  override def applyInternal(plan: LogicalPlan): LogicalPlan = plan match {
    case insert: InsertIntoHadoopFsRelationCommand
        if insert.query.resolved &&
          insert.bucketSpec.isEmpty && insert.catalogTable.isDefined &&
          isZorderEnabled(insert.catalogTable.get.properties) =>
      val dynamicPartition =
        insert.partitionColumns.filterNot(attr => insert.staticPartitions.contains(attr.name))
      val newQuery = insertZorder(insert.catalogTable.get, insert.query, dynamicPartition)
      if (newQuery.eq(insert.query)) {
        insert
      } else {
        insert.copy(query = newQuery)
      }

    case _ => plan
  }
}

case class InsertZorderBeforeWritingHive(session: SparkSession)
  extends InsertZorderHelper {
  override def applyInternal(plan: LogicalPlan): LogicalPlan = plan match {
    case insert: InsertIntoHiveTable
        if insert.query.resolved &&
          insert.table.bucketSpec.isEmpty && isZorderEnabled(insert.table.properties) =>
      val dynamicPartition = insert.partition.filter(_._2.isEmpty).keys
        .flatMap(name => insert.query.output.find(_.name == name)).toSeq
      val newQuery = insertZorder(insert.table, insert.query, dynamicPartition)
      if (newQuery.eq(insert.query)) {
        insert
      } else {
        insert.copy(query = newQuery)
      }

    case _ => plan
  }
}
