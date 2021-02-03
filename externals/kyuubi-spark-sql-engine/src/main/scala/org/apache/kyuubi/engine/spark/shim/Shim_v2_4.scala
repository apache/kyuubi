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

package org.apache.kyuubi.engine.spark.shim

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogPlugin

class Shim_v2_4 extends SparkShim {

  override def getCatalogs(spark: SparkSession): Seq[Row] = Seq(Row(""))

  override protected def getCatalog(spark: SparkSession, catalog: String): CatalogPlugin = null

  override protected def catalogExists(spark: SparkSession, catalog: String): Boolean = false

  override def getSchemas(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String): Seq[Row] = {
    (spark.sessionState.catalog.listDatabases(schemaPattern) ++
      getGlobalTempViewManager(spark, schemaPattern)).map(Row(_, ""))
  }

  override protected def getGlobalTempViewManager(
      spark: SparkSession, schemaPattern: String): Seq[String] = {
    val database = spark.sharedState.globalTempViewManager.database
    Option(database).filter(_.matches(schemaPattern)).toSeq
  }

  override def getCatalogTablesOrViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      tableTypes: Set[String]): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val databases = catalog.listDatabases(schemaPattern)

    databases.flatMap { db =>
      val identifiers = catalog.listTables(db, tablePattern, includeLocalTempViews = false)
      catalog.getTablesByName(identifiers)
        .filter(t => matched(tableTypes, t.tableType.name)).map { t =>
        val typ = if (t.tableType.name == "VIEW") "VIEW" else "TABLE"
        Row(catalogName, t.database, t.identifier.table, typ, t.comment.getOrElse(""),
          null, null, null, null, null)
      }
    }
  }

  override def getTempViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String): Seq[Row] = {
    val views = getViews(spark, schemaPattern, tablePattern)
    views.map { ident =>
      Row(catalogName, ident.database.orNull, ident.table, "VIEW", "",
        null, null, null, null, null)
    }
  }

  override protected def getViews(
      spark: SparkSession,
      schemaPattern: String,
      tablePattern: String): Seq[TableIdentifier] = {
    val db = getGlobalTempViewManager(spark, schemaPattern)
    if (db.nonEmpty) {
      spark.sessionState.catalog.listTables(db.head, tablePattern)
    } else {
      spark.sessionState.catalog.listLocalTempViews(tablePattern)
    }
  }
}
