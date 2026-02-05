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
  //                                          Catalog                                             //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Note that the result only contains loaded catalogs because catalogs are lazily loaded in Spark.
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
    // SPARK-36841 (3.3.0) Ensure setCurrentCatalog method catalog must exist
    if (spark.sessionState.catalogManager.isCatalogRegistered(catalog)) {
      spark.sessionState.catalogManager.setCurrentCatalog(catalog)
    } else {
      throw new IllegalArgumentException(s"Cannot find catalog plugin class for catalog '$catalog'")
    }
  }

  // SPARK-50700 (4.0.0) adds the `builtin` magic value
  private def hasCustomSessionCatalog(spark: SparkSession): Boolean = {
    spark.conf.get(s"spark.sql.catalog.$SESSION_CATALOG", "builtin") != "builtin"
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                           Schema                                             //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Return a list of [[Row]]s, with 2 fields `schemaName: String, catalogName: String`
   */
  def getSchemas(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String): Seq[Row] = {
    if (catalogName == SESSION_CATALOG && !hasCustomSessionCatalog(spark)) {
      val dbs = spark.sessionState.catalog.listDatabases(schemaPattern)
      val globalTempDb = getGlobalTempViewManager(spark, schemaPattern)
      (dbs ++ globalTempDb).map(Row(_, SESSION_CATALOG))
    } else {
      val catalog = getCatalog(spark, catalogName)
      getSchemasWithPattern(catalog, schemaPattern).map(Row(_, catalog.name))
    }
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
      tableTypes: Set[String],
      ignoreTableProperties: Boolean = false): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)
    val namespaces = listNamespacesWithPattern(catalog, schemaPattern)
    catalog match {
      case tc: TableCatalog =>
        val tp = tablePattern.r.pattern
        val identifiers = namespaces.flatMap { ns =>
          tc.listTables(ns).filter(i => tp.matcher(quoteIfNeeded(i.name())).matches())
        }
        identifiers.map { ident =>
          // TODO: restore view type for session catalog
          val comment = if (ignoreTableProperties) ""
          else { // load table is a time consuming operation
            tc.loadTable(ident).properties().getOrDefault(TableCatalog.PROP_COMMENT, "")
          }
          val schema = ident.namespace().map(quoteIfNeeded).mkString(".")
          val tableName = quoteIfNeeded(ident.name())
          Row(catalog.name(), schema, tableName, TABLE, comment, null, null, null, null, null)
        }
      case builtin if builtin.name() == SESSION_CATALOG =>
        val sessionCatalog = spark.sessionState.catalog
        val databases = sessionCatalog.listDatabases(schemaPattern)

        def isMatchedTableType(tableTypes: Set[String], tableType: String): Boolean = {
          val typ = if (tableType.equalsIgnoreCase(VIEW)) VIEW else TABLE
          tableTypes.exists(typ.equalsIgnoreCase)
        }

        databases.flatMap { db =>
          val identifiers =
            sessionCatalog.listTables(db, tablePattern, includeLocalTempViews = false)
          if (ignoreTableProperties) {
            identifiers.map { ti: TableIdentifier =>
              Row(
                catalogName,
                ti.database.getOrElse("default"),
                ti.table,
                TABLE, // ignore tableTypes criteria and simply treat all table type as TABLE
                "",
                null,
                null,
                null,
                null,
                null)
            }
          } else {
            sessionCatalog.getTablesByName(identifiers)
              .filter(t => isMatchedTableType(tableTypes, t.tableType.name)).map { t =>
                val typ = if (t.tableType.name == VIEW) VIEW else TABLE
                Row(
                  catalogName,
                  t.database,
                  t.identifier.table,
                  typ,
                  t.comment.getOrElse(""),
                  null,
                  null,
                  null,
                  null,
                  null)
              }
          }
        }
      case _ => Seq.empty[Row]
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

  def getTempViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String): Seq[Row] = {
    val views = getViews(spark, schemaPattern, tablePattern)
    views.map { ident =>
      Row(catalogName, ident.database.orNull, ident.table, VIEW, "", null, null, null, null, null)
    }
  }

  private def getViews(
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
