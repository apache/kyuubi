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
package org.apache.kyuubi.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.CreateTable

import org.apache.kyuubi.sql.KyuubiSQLConf.MOCK_OUTPUT_TABLE

case class MockOutputTable(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(MOCK_OUTPUT_TABLE)) {
      plan
    } else {
      plan resolveOperators {
        // InsertInto
        case i @ InsertIntoStatement(r: HiveTableRelation, _, _, _, _, _, _)
            if !isMockTable(r.tableMeta) =>
          val newTable = mockTable(r.tableMeta, true)
          i.copy(table = DDLUtils.readHiveTable(newTable))
        // CTAS
        case c @ CreateTable(tableDesc, _, Some(query))
            if !isMockTable(tableDesc) && query.resolved =>
          val newTable = mockTable(tableDesc, false)
          c.copy(tableDesc = newTable)
      }
    }
  }

  private def mockTable(sourceTable: CatalogTable, needCreateTable: Boolean): CatalogTable = {
    val newIdentifier = sourceTable.identifier
      .copy(table = mockTableName(sourceTable.identifier.table))

    val newStorage = sourceTable.storage.copy(locationUri = None)

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType = if (newStorage.locationUri.isEmpty) {
      CatalogTableType.MANAGED
    } else {
      CatalogTableType.EXTERNAL
    }

    val newTableSchema = CharVarcharUtils.getRawSchema(
      sourceTable.schema,
      spark.sessionState.conf)
    val newTable =
      CatalogTable(
        identifier = newIdentifier,
        tableType = tblType,
        storage = newStorage,
        schema = newTableSchema,
        provider = sourceTable.provider,
        partitionColumnNames = sourceTable.partitionColumnNames,
        bucketSpec = sourceTable.bucketSpec,
        tracksPartitionsInCatalog = sourceTable.tracksPartitionsInCatalog)

    if (needCreateTable) {
      spark.sessionState.catalog.createTable(newTable, true)
      spark.sessionState.catalog.getTableMetadata(newIdentifier)
    } else {
      newTable
    }
  }

  private def isMockTable(table: CatalogTable): Boolean = {
    val pattern = "(.*)_mock_\\d+$".r
    pattern.findFirstIn(table.identifier.table).isDefined
  }

  private def mockTableName(rawName: String): String = {
    s"${rawName}_mock_${System.currentTimeMillis()}"
  }

}
