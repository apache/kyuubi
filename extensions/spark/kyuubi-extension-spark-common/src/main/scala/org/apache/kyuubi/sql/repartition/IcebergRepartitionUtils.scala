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

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.util.reflect.ReflectUtils.{invokeAs, isClassLoadable}

object IcebergRepartitionUtils {
  private lazy val isIcebergSupported =
    isClassLoadable("org.apache.iceberg.spark.SparkCatalog")

  def getDynamicPartitionColsFromIcebergTable(
      table: NamedRelation,
      query: LogicalPlan): Option[Seq[Attribute]] = {
    if (!isIcebergSupported) {
      None
    } else {
      try {
        // [[org.apache.iceberg.spark.source.SparkTable]]
        val destIcebergTable = invokeAs[AnyRef](table, "table")
        val partitionCols = invokeAs[Array[AnyRef]](destIcebergTable, "partitioning")
        if (partitionCols.isEmpty) {
          // use first column of output as repartition column for non-partitioned table
          query.output.headOption.map(Seq(_))
        } else {
          val partitionNames = partitionCols.map(p => {
            val ref = invokeAs[AnyRef](p, "ref")
            val refName = invokeAs[Iterable[String]](ref, "parts").mkString(".")
            refName
          })
          val dynamicPartitionColumns =
            query.output.attrs.filter(attr => partitionNames.contains(attr.name))
          Some(dynamicPartitionColumns)
        }
      } catch {
        case _: Exception => None
      }
    }
  }
}
