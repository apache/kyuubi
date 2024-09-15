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
package org.apache.spark.sql

import scala.util.Random

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, RepartitionByExpression}
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.compact._

class CompactTableResolverStrategySuite extends KyuubiSparkSQLExtensionTest {

  def createRandomTable(): String = {
    val tableName = s"small_file_table_${Random.alphanumeric.take(10).mkString}"
    spark.sql(s"CREATE TABLE ${tableName} (key INT, val_str STRING) USING csv").show()
    tableName
  }

  test("compact table execution plan") {
    val tableName = createRandomTable()
    withTable(tableName) {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val result = spark.sql(s"compact table ${tableName}")
        result.show()
        val groupId = CompactTable.smallFileCollectOutputAttribute.head
        result.queryExecution.analyzed match {
          case CachePerformanceViewCommand(
                Seq(CompactTable.mergedFilesCachedTableName),
                SmallFileMerge(
                  RepartitionByExpression(
                    Seq(AttributeReference(groupId.name, groupId.dataType, groupId.nullable, _)),
                    SmallFileCollect(LogicalRelation(_, _, Some(table), _), None),
                    None,
                    None),
                  false),
                Seq(originalFileLocation),
                CompactTableOptions.CleanupStagingFolder)
              if table.identifier.table == tableName
                && table.location.toString == originalFileLocation => // ok
          case CachePerformanceViewCommand(
                Seq(CompactTable.mergedFilesCachedTableName),
                SmallFileMerge(
                  RepartitionByExpression(
                    Seq(AttributeReference(groupId.name, groupId.dataType, groupId.nullable, _)),
                    SmallFileCollect(LogicalRelation(_, _, Some(table), _), None),
                    None,
                    None),
                  false),
                Seq(originalFileLocation),
                CompactTableOptions.CleanupStagingFolder
              ) => // not ok
            log.info(s"result.queryExecution.analyzed: ${result.queryExecution.analyzed}")
          case other => fail(s"Unexpected plan: $other, should be CachePerformanceViewCommand")
        }

        result.queryExecution.optimizedPlan match {
          case CommandResult(
                _,
                CachePerformanceViewCommand(
                  Seq(CompactTable.mergedFilesCachedTableName),
                  SmallFileMerge(
                    RepartitionByExpression(
                      Seq(AttributeReference(groupId.name, groupId.dataType, groupId.nullable, _)),
                      SmallFileCollect(LogicalRelation(_, _, Some(table), _), None),
                      None,
                      None),
                    false),
                  Seq(originalFileLocation),
                  CompactTableOptions.CleanupStagingFolder),
                _,
                Seq())
              if table.identifier.table == tableName
                && originalFileLocation == table.location.toString => // ok
          case other => fail(s"Unexpected plan: $other, should be CachePerformanceViewCommand")
        }

        result.queryExecution.executedPlan match {
          case CommandResultExec(
                output,
                ExecutedCommandExec(CachePerformanceViewCommand(
                  Seq(CompactTable.mergedFilesCachedTableName),
                  SmallFileMerge(
                    RepartitionByExpression(
                      Seq(AttributeReference(groupId.name, groupId.dataType, groupId.nullable, _)),
                      SmallFileCollect(LogicalRelation(_, _, Some(table), _), None),
                      None,
                      None),
                    false),
                  Seq(originalFileLocation),
                  CompactTableOptions.CleanupStagingFolder)),
                Seq())
              if table.identifier.table == tableName
                && table.location.toString == originalFileLocation => // ok
          case other => fail(s"Unexpected plan: $other, should be CachePerformanceViewCommand")
        }
      }
    }
  }

  test("recover compact table execution plan") {
    val tableName = createRandomTable()
    withTable(tableName) {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val result = spark.sql(s"recover compact table ${tableName}")
        result.show()
        result.queryExecution.analyzed match {
          case RecoverCompactTableCommand(catalogTable: CatalogTable)
              if catalogTable.identifier.table == tableName => // ok
          case other => fail(s"Unexpected plan: $other, should be RecoverCompactTableCommand")
        }

        result.queryExecution.optimizedPlan match {
          case CommandResult(_, RecoverCompactTableCommand(catalogTable: CatalogTable), _, Seq())
              if catalogTable.identifier.table == tableName => // ok
          case other => fail(s"Unexpected plan: $other, should be RecoverCompactTableCommand")
        }

        result.queryExecution.executedPlan match {
          case CommandResultExec(
                _,
                ExecutedCommandExec(RecoverCompactTableCommand(catalogTable: CatalogTable)),
                Seq())
              if catalogTable.identifier.table == tableName => // ok
          case other => fail(s"Unexpected plan: $other, should be RecoverCompactTableCommand")
        }

      }
    }
  }
}
