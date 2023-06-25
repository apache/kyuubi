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

package org.apache.kyuubi.spark.connector.hive.write

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.{AlwaysTrue, And, EqualNullSafe, EqualTo, Filter}

import org.apache.kyuubi.spark.connector.hive.{HiveTableCatalog, KyuubiHiveConnectorException}

case class HiveWriteBuilder(
    sparkSession: SparkSession,
    catalogTable: CatalogTable,
    info: LogicalWriteInfo,
    hiveTableCatalog: HiveTableCatalog) extends WriteBuilder with SupportsOverwrite
  with SupportsDynamicOverwrite {

  private var forceOverwrite = false

  private var staticPartition: TablePartitionSpec = Map.empty

  private val ifPartitionNotExists = false

  private val parts = catalogTable.partitionColumnNames

  override def build(): Write = {
    HiveWrite(
      sparkSession,
      catalogTable,
      info,
      hiveTableCatalog,
      forceOverwrite,
      mergePartitionSpec(),
      ifPartitionNotExists)
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    filters match {
      case Array(AlwaysTrue) => // no partition, do nothing
      case _ => staticPartition = deduplicateFilters(filters)
    }
    overwriteDynamicPartitions()
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    forceOverwrite = true
    this
  }

  private def mergePartitionSpec(): Map[String, Option[String]] = {
    var partSpec = Map.empty[String, Option[String]]

    staticPartition.foreach {
      case (p, v) => partSpec = partSpec.updated(p, Some(v))
    }

    val dynamicCols = parts diff staticPartition.keySet.toSeq
    dynamicCols.foreach(p => partSpec = partSpec.updated(p, None))
    partSpec
  }

  private def deduplicateFilters(filters: Array[Filter]): TablePartitionSpec = {
    filters.map(extractConjunctions).reduce((partDesc1, partDesc2) => partDesc1 ++ partDesc2)
  }

  private def extractConjunctions(filter: Filter): TablePartitionSpec = {
    filter match {
      case And(l, r) => extractConjunctions(l) ++ extractConjunctions(r)
      case EqualNullSafe(att, value) => Map(att -> value.toString)
      case EqualTo(att, value) => Map(att -> value.toString)
      case _ => throw KyuubiHiveConnectorException(s"Unsupported static insert condition $filter")
    }
  }
}
