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

package org.apache.kyuubi.sql.repartition

import java.util.{Map => JMap}

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}

import org.apache.kyuubi.util.reflect.ReflectUtils._

object IcebergRepartitionUtils {
  private lazy val maybeIcebergSparkTableClass =
    loadClassOpt[Table]("org.apache.iceberg.spark.source.SparkTable")

  private lazy val isIcebergSupported = maybeIcebergSparkTableClass.isDefined

  def getDynamicPartitionColsFromIcebergTable(
      table: NamedRelation,
      query: LogicalPlan): Option[Seq[Attribute]] = {
    if (!isIcebergSupported) {
      return None
    }

    invokeAsOpt[Table](table, "table") match {
      case Some(destIcebergTable) if shouldApplyToIcebergTable(destIcebergTable) =>
        val partitionCols = invokeAs[Array[_ <: Transform]](destIcebergTable, "partitioning")
        val partitionColNames = partitionCols.map(col => {
          val ref = invokeAs[NamedReference](col, "ref")
          ref.fieldNames().mkString(".")
        })
        val dynamicPartitionColumns =
          query.output.attrs.filter(attr => partitionColNames.contains(attr.name))
        Some(dynamicPartitionColumns)
      case _ => None
    }
  }

  private def isIcebergSparkTable(table: Table) =
    maybeIcebergSparkTableClass.isDefined && maybeIcebergSparkTableClass.get.isInstance(table)

  private def shouldApplyToIcebergTable(table: Table): Boolean = {
    table match {
      case icebergTable if isIcebergSparkTable(icebergTable) =>
        val properties = invokeAs[JMap[String, String]](icebergTable, "properties")
        val isUseTableDistributionAndOrdering =
          "true".equalsIgnoreCase(properties.get("use-table-distribution-and-ordering"))
        // skipping repartitioning for Iceberg table with distribution and ordering
        !isUseTableDistributionAndOrdering
      case _ => false
    }
  }
}
