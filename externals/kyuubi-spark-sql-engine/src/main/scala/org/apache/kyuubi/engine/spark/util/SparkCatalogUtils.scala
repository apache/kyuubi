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

package org.apache.kyuubi.engine.spark.util

import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.types.StructField
import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.schema.SchemaHelper
import org.apache.kyuubi.util.reflect.ReflectUtils._

/**
 * A shim that defines the interface interact with Spark's catalogs
 */
object SparkCatalogUtils extends Logging {

  private val VIEW = "VIEW"
  private val TABLE = "TABLE"

  val SESSION_CATALOG: String = "spark_catalog"
  val sparkTableTypes: Set[String] = Set(VIEW, TABLE)

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Catalog                                            //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get all register catalogs in Spark's `CatalogManager`
   */
  def getCatalogs(spark: SparkSession): Seq[Row] = {

    // A [[CatalogManager]] is session unique
    val catalogMgr = spark.sessionState.catalogManager
    // get the custom v2 session catalog or default spark_catalog
    val sessionCatalog = invokeAs[AnyRef](catalogMgr, "v2SessionCatalog")
    val defaultCatalog = catalogMgr.currentCatalog

    val defaults = Seq(sessionCatalog, defaultCatalog).distinct.map(invokeAs[String](_, "name"))
    val catalogs = getField[scala.collection.Map[String, _]](catalogMgr, "catalogs")
    (catalogs.keys ++: defaults).distinct.map(Row(_))
  }

  def getCatalog(spark: SparkSession, catalogName: String): CatalogPlugin = {
    val catalogManager = spark.sessionState.catalogManager
    if (StringUtils.isBlank(catalogName)) {
      catalogManager.currentCatalog
    } else {
      catalogManager.catalog(catalogName)
    }
  }

  def setCurrentCatalog(spark: SparkSession, catalog: String): Unit = {
    // SPARK-36841(3.3.0) Ensure setCurrentCatalog method catalog must exist
    if (spark.sessionState.catalogManager.isCatalogRegistered(catalog)) {
      spark.sessionState.catalogManager.setCurrentCatalog(catalog)
    } else {
      throw new IllegalArgumentException(s"Cannot find catalog plugin class for catalog '$catalog'")
    }
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                           Schema                                            //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * a list of [[Row]]s, with 2 fields `schemaName: String, catalogName: String`
   */
  def getSchemas(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)
    val showSql = s"SHOW DATABASES IN ${catalog.name} LIKE '${schemaPattern}'"
    val databaseResult = spark.sql(showSql)
    val databases = databaseResult.collect().toSeq
    val globalTempViewsAsRows = getGlobalTempViewManager(spark, schemaPattern).map(Row(_))
    (databases ++ globalTempViewsAsRows).map(row => Row(row(0), catalog.name))
  }

  private def getGlobalTempViewManager(
      spark: SparkSession,
      schemaPattern: String): Seq[String] = {
    val database = spark.conf.get("spark.sql.globalTempDatabase")
    Option(database).filter(_.matches(schemaPattern)).toSeq
  }

  private def listAllNamespaces(
      catalog: SupportsNamespaces,
      namespaces: Array[Array[String]]): Array[Array[String]] = {
    val children = namespaces.flatMap { ns =>
      catalog.listNamespaces(ns)
    }
    if (children.isEmpty) {
      namespaces
    } else {
      namespaces ++: listAllNamespaces(catalog, children)
    }
  }

  private def listAllNamespaces(catalog: CatalogPlugin): Array[Array[String]] = {
    catalog match {
      case catalog: CatalogExtension =>
        // DSv2 does not support pass schemaPattern transparently
        catalog.defaultNamespace() +: catalog.listNamespaces(Array())
      case catalog: SupportsNamespaces =>
        val rootSchema = catalog.listNamespaces()
        val allSchemas = listAllNamespaces(catalog, rootSchema)
        allSchemas
    }
  }

  private def listNamespacesWithPattern(
      catalog: CatalogPlugin,
      schemaPattern: String): Array[Array[String]] = {
    listAllNamespaces(catalog).filter { ns =>
      val quoted = ns.map(quoteIfNeeded).mkString(".")
      schemaPattern.r.pattern.matcher(quoted).matches()
    }.map(_.toList).toList.distinct.map(_.toArray).toArray
  }

  private def getSchemasWithPattern(catalog: CatalogPlugin, schemaPattern: String): Seq[String] = {
    val p = schemaPattern.r.pattern
    listAllNamespaces(catalog).flatMap { ns =>
      val quoted = ns.map(quoteIfNeeded).mkString(".")
      if (p.matcher(quoted).matches()) Some(quoted) else None
    }.distinct
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                        Table & View                                         //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  def getCatalogTablesOrViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      ignoreTableProperties: Boolean = false): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)
    val databases = getSchemas(spark, catalog.name, schemaPattern)
    databases.flatMap {
      row =>
        try {
          val database = row.getString(0)
          val catalogRes = row.getString(1)
          val showSql = s"SHOW TABLES IN ${catalogRes}.${database} LIKE '${tablePattern}'"
          val tables = spark.sql(showSql).collect().toSeq
          tables.map { row =>
            val table = row.getString(1)
            if (ignoreTableProperties) {
              Row(
                catalogRes,
                database,
                table,
                TABLE,
                "",
                null,
                null,
                null,
                null,
                null)
            } else {
              val descSql = s"DESC EXTENDED ${catalogRes}.${database}.${table}"
              val tblInfo = spark.sql(descSql).collect
              var tblType = ""
              var comment = ""
              tblInfo.foreach { row =>
                val elements = row.toSeq
                if (elements.length >= 2 && elements(0).toString.equalsIgnoreCase("type")) {
                  tblType = elements(1).toString
                }
                if (elements.length >= 2 && elements(0).toString.equalsIgnoreCase("comment")) {
                  comment = elements(1).toString
                }
              }
              val typ = if (tblType.equalsIgnoreCase(VIEW)) VIEW else TABLE
              Row(
                catalogRes,
                database,
                table,
                typ,
                comment,
                null,
                null,
                null,
                null,
                null)
            }
          }
        } catch {
          case e: Exception =>
            error(s"Failed to get tables from catalog $catalogName", e)
            Seq.empty[Row]
        }
    }
  }

  private def getColumnsByCatalog(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)

    catalog match {
      case tc: TableCatalog =>
        val namespaces = listNamespacesWithPattern(catalog, schemaPattern)
        val tp = tablePattern.r.pattern
        val identifiers = namespaces.flatMap { ns =>
          tc.listTables(ns).filter(i => tp.matcher(quoteIfNeeded(i.name())).matches())
        }
        identifiers.flatMap { ident =>
          val table = tc.loadTable(ident)
          val namespace = ident.namespace().map(quoteIfNeeded).mkString(".")
          val tableName = quoteIfNeeded(ident.name())

          table.schema.zipWithIndex.filter(f => columnPattern.matcher(f._1.name).matches())
            .map { case (f, i) => toColumnResult(tc.name(), namespace, tableName, f, i) }
        }

      case builtin if builtin.name() == SESSION_CATALOG =>
        val catalog = spark.sessionState.catalog
        val databases = catalog.listDatabases(schemaPattern)
        databases.flatMap { db =>
          val identifiers = catalog.listTables(db, tablePattern, includeLocalTempViews = true)
          catalog.getTablesByName(identifiers).flatMap { t =>
            t.schema.zipWithIndex.filter(f => columnPattern.matcher(f._1.name).matches())
              .map { case (f, i) =>
                toColumnResult(catalogName, t.database, t.identifier.table, f, i)
              }
          }
        }
    }
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Columns                                            //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  def getColumns(
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

  private def getColumnsByGlobalTempViewManager(
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
              toColumnResult(SparkCatalogUtils.SESSION_CATALOG, globalTmpDb, v, f, i)
            }
        }
      }.flatten
    }
  }

  private def getColumnsByLocalTempViews(
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
            toColumnResult(SparkCatalogUtils.SESSION_CATALOG, null, v.table, f, i)
          }
      }
  }

  private def toColumnResult(
      catalog: String,
      db: String,
      table: String,
      col: StructField,
      pos: Int): Row = {
    // format: off
    Row(
      catalog,                                              // TABLE_CAT
      db,                                                   // TABLE_SCHEM
      table,                                                // TABLE_NAME
      col.name,                                             // COLUMN_NAME
      SchemaHelper.toJavaSQLType(col.dataType),             // DATA_TYPE
      col.dataType.sql,                                     // TYPE_NAME
      SchemaHelper.getColumnSize(col.dataType).orNull,      // COLUMN_SIZE
      null,                                                 // BUFFER_LENGTH
      SchemaHelper.getDecimalDigits(col.dataType).orNull,   // DECIMAL_DIGITS
      SchemaHelper.getNumPrecRadix(col.dataType).orNull,    // NUM_PREC_RADIX
      if (col.nullable) 1 else 0,                           // NULLABLE
      col.getComment().getOrElse(""),                       // REMARKS
      null,                                                 // COLUMN_DEF
      null,                                                 // SQL_DATA_TYPE
      null,                                                 // SQL_DATETIME_SUB
      null,                                                 // CHAR_OCTET_LENGTH
      pos,                                                  // ORDINAL_POSITION
      "YES",                                                // IS_NULLABLE
      null,                                                 // SCOPE_CATALOG
      null,                                                 // SCOPE_SCHEMA
      null,                                                 // SCOPE_TABLE
      null,                                                 // SOURCE_DATA_TYPE
      "NO"                                                  // IS_AUTO_INCREMENT
    )
    // format: on
  }

  // SPARK-47300 (4.0.0): quoteIfNeeded should quote identifier starts with digits
  private val validIdentPattern = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*")

  // Forked from Apache Spark's [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIfNeeded]]
  def quoteIfNeeded(part: String): String = {
    if (validIdentPattern.matcher(part).matches()) {
      part
    } else {
      quoteIdentifier(part)
    }
  }

  // Forked from Apache Spark's [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier]]
  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }
}
