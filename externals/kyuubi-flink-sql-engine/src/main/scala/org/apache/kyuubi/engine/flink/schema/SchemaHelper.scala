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

package org.apache.kyuubi.engine.flink.schema

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.flink.table.catalog.{Catalog, CatalogBaseTable, ObjectIdentifier}

object SchemaHelper {

  def getSchemasWithPattern(flinkCatalog: Catalog, schemaNamePattern: String): Array[String] = {
    val p = schemaNamePattern.r.pattern
    flinkCatalog.listDatabases().asScala.toArray
      .filter { schemaName => p.matcher(schemaName).matches() }
  }

  def getTablesWithPattern(
      flinkCatalog: Catalog,
      schemaName: String,
      tableNamePattern: String): Array[String] = {
    val p = tableNamePattern.r.pattern
    flinkCatalog.listTables(schemaName).asScala.toArray
      .filter { tableName => p.matcher(tableName).matches() }
  }

  def getFlinkTablesWithPattern(
      flinkCatalog: Catalog,
      catalogName: String,
      schemaName: String,
      tableNamePattern: String): Array[(String, Option[CatalogBaseTable])] = {
    getTablesWithPattern(flinkCatalog, schemaName, tableNamePattern).map { tableName =>
      getFlinkTable(flinkCatalog, catalogName, schemaName, tableName)
    }
  }

  def getFlinkTable(
      flinkCatalog: Catalog,
      catalogName: String,
      schemaName: String,
      tableName: String): (String, Option[CatalogBaseTable]) = {
    val objPath = ObjectIdentifier.of(catalogName, schemaName, tableName).toObjectPath
    Try(flinkCatalog.getTable(objPath)) match {
      case Success(flinkTable) => (tableName, Some(flinkTable))
      case Failure(_) => (tableName, None)
    }
  }
}
