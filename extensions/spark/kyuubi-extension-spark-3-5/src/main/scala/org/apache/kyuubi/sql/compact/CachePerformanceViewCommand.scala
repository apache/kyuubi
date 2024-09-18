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

package org.apache.kyuubi.sql.compact

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SparkInternalExplorer, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{DropTableCommand, LeafRunnableCommand}

case class CachePerformanceViewCommand(
    tableIdentifier: Seq[String],
    performancePlan: LogicalPlan,
    originalFileLocations: Seq[String],
    options: CompactTableOption) extends LeafRunnableCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(performancePlan)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dropViewCommand = DropTableCommand(
      CompactTableUtils.getTableIdentifier(tableIdentifier),
      ifExists = true,
      isView = true,
      purge = true)
    dropViewCommand.run(sparkSession)

    val speculation =
      sparkSession.sparkContext.getConf.getBoolean(
        SparkInternalExplorer.SPECULATION_ENABLED_SYNONYM.key,
        defaultValue = false)
    if (speculation) {
      sparkSession.sparkContext.getConf.set(
        SparkInternalExplorer.SPECULATION_ENABLED_SYNONYM.key,
        "false")
      log.warn("set spark.speculation to false")
    }
    try {
      val cacheTableCommand =
        SparkInternalExplorer.CacheTableAsSelectExec(tableIdentifier.head, performancePlan)

      // this result always empty
      cacheTableCommand.run()

      if (options == CompactTableOptions.CleanupStagingFolder) {
        val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
        originalFileLocations.foreach { originalFileLocation =>
          val compactStagingDir = CompactTableUtils.getCompactStagingDir(originalFileLocation)
          fileSystem.delete(compactStagingDir, true)
        }

      }
    } finally {
      if (speculation) {
        sparkSession.sparkContext.getConf.set(
          SparkInternalExplorer.SPECULATION_ENABLED_SYNONYM.key,
          "true")
        log.warn("rollback spark.speculation to true")
      }
    }
    Seq.empty[Row]
  }

}
