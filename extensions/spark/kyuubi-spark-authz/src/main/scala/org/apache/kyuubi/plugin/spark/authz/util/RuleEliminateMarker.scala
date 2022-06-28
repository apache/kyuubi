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

package org.apache.kyuubi.plugin.spark.authz.util

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.internal.SQLConf

class RuleEliminateMarker(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case rf: RowFilterAndDataMaskingMarker =>
        val table = rf.table
        table match {
          case relation: HiveTableRelation =>
            if (DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty) {
              hiveTableWithStats(relation)
            } else {
              relation
            }
          case _ =>
            table
        }
    }
  }

  private def hiveTableWithStats(relation: HiveTableRelation): HiveTableRelation = {
    val conf = SQLConf.get
    val table = relation.tableMeta
    val partitionCols = relation.partitionCols
    // For partitioned tables, the partition directory may be outside of the table directory.
    // Which is expensive to get table size. Please see how we implemented it in the AnalyzeTable.
    val sizeInBytes =
      if (conf.fallBackToHdfsForStatsEnabled && partitionCols.isEmpty) {
        try {
          val hadoopConf = spark.sessionState.newHadoopConf()
          val tablePath = new Path(table.location)
          val fs: FileSystem = tablePath.getFileSystem(hadoopConf)
          fs.getContentSummary(tablePath).getLength
        } catch {
          case e: IOException =>
            logWarning("Failed to get table size from HDFS.", e)
            conf.defaultSizeInBytes
        }
      } else {
        conf.defaultSizeInBytes
      }

    val stats = Some(Statistics(sizeInBytes = BigInt(sizeInBytes)))
    relation.copy(tableStats = stats)
  }
}
