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

import java.util.regex.Pattern

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException

class CatalogShim_v2_4 extends SparkCatalogShim {

  override def getCatalogs(spark: SparkSession): Seq[Row] = {
    Seq(Row(SparkCatalogShim.SESSION_CATALOG))
  }

  override protected def catalogExists(spark: SparkSession, catalog: String): Boolean = false

  override def getSchemas(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String): Seq[Row] = {
    (getSchemas(spark, schemaPattern) ++
      getGlobalTempViewManager(spark, schemaPattern)).map(Row(_, ""))
  }

  protected def getSchemas(spark: SparkSession, schemaPattern: String): Seq[String] = {
    val showDatabases = "SHOW DATABASES"
    spark.sql(showDatabases).collect().filter(r => r.getString(0).matches(schemaPattern))
      .map(r => r.getString(0))
  }

  override protected def getGlobalTempViewManager(
      spark: SparkSession,
      schemaPattern: String): Seq[String] = {
    val database = spark.sharedState.globalTempViewManager.database
    Option(database).filter(_.matches(schemaPattern)).toSeq
  }

  override def getCatalogTablesOrViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      tableTypes: Set[String]): Seq[Row] = {
    getSchemas(spark, schemaPattern).flatMap(db => {
      tableTypes.flatMap(t => {
        getCatalogTablesOrViews(spark, t, db, tablePattern)
      }).toSeq
    })
  }

  private def getCatalogTablesOrViews(
      spark: SparkSession,
      tableType: String,
      db: String,
      tablePattern: String): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val tp = tablePattern.r.pattern

    var showSql = s"SHOW TABLES IN $db"
    if (tableType.equals("VIEW")) {
      showSql = s"SHOW VIEWS IN $db"
    }

    spark.sql(showSql).collect()
      .filter(r => tp.matcher(r.getString(1)).matches())
      .flatMap(r => {
        val database = r.getString(0)
        val tableName = r.getString(1)
        val identifier = new TableIdentifier(tableName, Option.apply(database))
        catalog.getTablesByName(Array(identifier)).map(f => {
          Row(
            SparkCatalogShim.SESSION_CATALOG,
            database,
            tableName,
            tableType,
            f.comment.getOrElse(""),
            null,
            null,
            null,
            null,
            null)
        })
      })
  }

  override def getTempViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String): Seq[Row] = {
    val views = getViews(spark, schemaPattern, tablePattern)
    views.map { ident =>
      Row(catalogName, ident.database.orNull, ident.table, "VIEW", "", null, null, null, null, null)
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

  override def getColumns(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: String): Seq[Row] = {

    val cp = columnPattern.r.pattern
    val byCatalog = getColumnsByCatalog(spark, catalogName, schemaPattern, tablePattern, cp)
    val byGlobalTmpDB = getColumnsByGlobalTempViewManager(spark, schemaPattern, tablePattern, cp)
    val byLocalTmp = getColumnsByLocalTempViews(spark, tablePattern, cp)

    byCatalog ++ byGlobalTmpDB ++ byLocalTmp
  }

  protected def getColumnsByCatalog(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = spark.sessionState.catalog

    val databases = getSchemas(spark, schemaPattern)

    val byCatalog = databases.flatMap(db => {
      val tables = getCatalogTablesOrViews(spark, catalogName, db, tablePattern)

      tables.flatMap(r => {
        val identifier = new TableIdentifier(r.getString(2), Option.apply(db))
        catalog.getTablesByName(Array(identifier)).flatMap(t => {
          val sqlText = s"SHOW COLUMNS IN $db.${identifier.table}"

          val columns =
            try {
              spark.sql(sqlText).collect().map(_.getString(0))
            } catch { // Possibly deleted by other threads
              case _: TableAlreadyExistsException => Array()
            }
          if (columns.isEmpty) {
            Seq.empty
          }

          val tableSchema =
            if (t.provider.getOrElse("").equalsIgnoreCase("delta")) {
              spark.table(t.identifier.table).schema
            } else {
              t.schema
            }
          tableSchema.zipWithIndex.filter(f =>
            columnPattern.matcher(f._1.name).matches()
              && columns.exists(_.equals(f._1.name)))
            .map({ case (f, i) =>
              toColumnResult(catalogName, t.database, t.identifier.table, f, i)
            })
        })
      })
    })

    byCatalog
  }

  protected def getColumnsByGlobalTempViewManager(
      spark: SparkSession,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = spark.sessionState.catalog

    getGlobalTempViewManager(spark, schemaPattern).flatMap { globalTmpDb =>
      catalog.globalTempViewManager.listViewNames(tablePattern).flatMap { v =>
        catalog.globalTempViewManager.get(v).map { plan =>
          plan.schema.zipWithIndex.filter(f => columnPattern.matcher(f._1.name).matches())
            .map { case (f, i) =>
              toColumnResult(SparkCatalogShim.SESSION_CATALOG, globalTmpDb, v, f, i)
            }
        }
      }.flatten
    }
  }

  protected def getColumnsByLocalTempViews(
      spark: SparkSession,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = spark.sessionState.catalog

    catalog.listLocalTempViews(tablePattern)
      .map(v => (v, catalog.getTempView(v.table).get))
      .flatMap { case (v, plan) =>
        plan.schema.zipWithIndex
          .filter(f => columnPattern.matcher(f._1.name).matches())
          .map { case (f, i) =>
            toColumnResult(SparkCatalogShim.SESSION_CATALOG, null, v.table, f, i)
          }
      }
  }
}
