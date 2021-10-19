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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

/**
 * A runnable command for zorder, we delegate to real command to execute
 */
abstract class OptimizeZorderCommandBase extends DataWritingCommand {
  def catalogTable: CatalogTable

  override def outputColumnNames: Seq[String] = query.output.map(_.name)

  private def isHiveTable: Boolean = {
    catalogTable.provider.isEmpty ||
      (catalogTable.provider.isDefined && "hive".equalsIgnoreCase(catalogTable.provider.get))
  }

  private def getWritingCommand(session: SparkSession): DataWritingCommand = {
    // TODO: Support convert hive relation to datasource relation, can see
    //  [[org.apache.spark.sql.hive.RelationConversions]]
    InsertIntoHiveTable(
      catalogTable,
      catalogTable.partitionColumnNames.map(p => (p, None)).toMap,
      query,
      overwrite = true,
      ifPartitionNotExists = false,
      outputColumnNames
    )
  }

  override def run(session: SparkSession, child: SparkPlan): Seq[Row] = {
    // TODO: Support datasource relation
    // TODO: Support read and insert overwrite the same table for some table format
    if (!isHiveTable) {
      throw new KyuubiSQLExtensionException("only support hive table")
    }

    val command = getWritingCommand(session)
    command.run(session, child)
    DataWritingCommand.propogateMetrics(session.sparkContext, command, metrics)
    Seq.empty
  }
}
