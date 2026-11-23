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

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, Identifier, SupportsNamespaces, TableCatalog}
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
  private val METRIC_VIEW = "METRIC_VIEW"

  val SESSION_CATALOG: String = "spark_catalog"
  val sparkTableTypes: Set[String] = Set(VIEW, TABLE)

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Catalog                                            //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  // SPARK-46050 (4.2.0) changed CatalogManager from a class to an interface, breaking
  // binary compatibility when compiled against Spark 3.5 and run against Spark 4.2.
  // Access catalogManager reflectively to avoid IncompatibleClassChangeError.
  def catalogManager(spark: SparkSession): AnyRef =
    invokeAs[AnyRef](spark.sessionState, "catalogManager")

  def setCurrentNamespace(spark: SparkSession, namespace: Array[String]): Unit = {
    val mgr = catalogManager(spark)
    val method = mgr.getClass.getMethod("setCurrentNamespace", classOf[Array[String]])
    try {
      method.invoke(mgr, namespace)
    } catch {
      case e: java.lang.reflect.InvocationTargetException => throw e.getCause
    }
  }

  def currentCatalog(spark: SparkSession): CatalogPlugin =
    invokeAs[CatalogPlugin](catalogManager(spark), "currentCatalog")

  def currentNamespace(spark: SparkSession): Array[String] =
    invokeAs[Array[String]](catalogManager(spark), "currentNamespace")

  def catalog(spark: SparkSession, name: String): CatalogPlugin =
    invokeAs[CatalogPlugin](catalogManager(spark), "catalog", (classOf[String], name))

  def isCatalogRegistered(spark: SparkSession, name: String): Boolean =
    invokeAs[Boolean](catalogManager(spark), "isCatalogRegistered", (classOf[String], name))

  /**
   * Get all register catalogs in Spark's `CatalogManager`
   */
  def getCatalogs(spark: SparkSession): Seq[Row] = {
    val catalogMgr = catalogManager(spark)
    // get the custom v2 session catalog or default spark_catalog
    val sessionCatalog = invokeAs[AnyRef](catalogMgr, "v2SessionCatalog")
    val defaultCatalog = invokeAs[CatalogPlugin](catalogMgr, "currentCatalog")

    val defaults = Seq(sessionCatalog, defaultCatalog).distinct.map(invokeAs[String](_, "name"))
    val catalogs = getField[scala.collection.Map[String, _]](catalogMgr, "catalogs")
    (catalogs.keys ++: defaults).distinct.map(Row(_))
  }

  def getCatalog(spark: SparkSession, catalogName: String): CatalogPlugin = {
    if (StringUtils.isBlank(catalogName)) {
      currentCatalog(spark)
    } else {
      catalog(spark, catalogName)
    }
  }

  def setCurrentCatalog(spark: SparkSession, catalog: String): Unit = {
    // SPARK-36841 (3.3.0) Ensure setCurrentCatalog method catalog must exist
    if (isCatalogRegistered(spark, catalog)) {
      invokeAs[Unit](catalogManager(spark), "setCurrentCatalog", (classOf[String], catalog))
    } else {
      throw new IllegalArgumentException(s"Cannot find catalog plugin class for catalog '$catalog'")
    }
  }

  // SPARK-50700 (4.0.0) adds the `builtin` magic value
  private def hasCustomSessionCatalog(spark: SparkSession): Boolean = {
    !spark.conf.get(s"spark.sql.catalog.$SESSION_CATALOG", "builtin").equalsIgnoreCase("builtin")
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
    if (catalog.name() == SESSION_CATALOG) {
      val dbs = if (hasCustomSessionCatalog(spark)) {
        getSchemasWithPattern(catalog, schemaPattern, quote = false)
      } else {
        spark.sessionState.catalog.listDatabases(schemaPattern)
      }
      (dbs ++ getGlobalTempViewManager(spark, schemaPattern))
        .map(Row(_, catalog.name()))
    } else {
      getSchemasWithPattern(catalog, schemaPattern).map(Row(_, catalog.name()))
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
      case _ =>
        Array.empty[Array[String]]
    }
  }

  private def compilePattern(catalog: CatalogPlugin, pattern: String): Pattern = {
    if (catalog.name() == SESSION_CATALOG) {
      Pattern.compile(pattern, Pattern.CASE_INSENSITIVE)
    } else {
      Pattern.compile(pattern)
    }
  }

  private def listNamespacesWithPattern(
      catalog: CatalogPlugin,
      schemaPattern: String,
      quote: Boolean = true): Array[Array[String]] = {
    val p = compilePattern(catalog, schemaPattern)
    listAllNamespaces(catalog).filter { ns =>
      val name = if (quote) ns.map(quoteIfNeeded).mkString(".") else ns.mkString(".")
      p.matcher(name).matches()
    }.map(_.toList).toList.distinct.map(_.toArray).toArray
  }

  private def getSchemasWithPattern(
      catalog: CatalogPlugin,
      schemaPattern: String,
      quote: Boolean = true): Seq[String] = {
    val p = compilePattern(catalog, schemaPattern)
    listAllNamespaces(catalog).flatMap { ns =>
      val name = if (quote) ns.map(quoteIfNeeded).mkString(".") else ns.mkString(".")
      if (p.matcher(name).matches()) Some(name) else None
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
    val quoteNamespaces = catalog.name() != SESSION_CATALOG
    val namespaces = listNamespacesWithPattern(catalog, schemaPattern, quote = quoteNamespaces)
    catalog match {
      case builtin if builtin.name() == SESSION_CATALOG && !hasCustomSessionCatalog(spark) =>
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
                catalog.name(),
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
                  catalog.name(),
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

      case tc: TableCatalog if tc.name() == SESSION_CATALOG =>
        getCustomSessionCatalogTablesOrViews(
          spark,
          tc,
          namespaces,
          tablePattern,
          tableTypes,
          ignoreTableProperties)

      case tc: TableCatalog =>
        val tp = tablePattern.r.pattern
        if (ignoreTableProperties) {
          namespaces.flatMap { ns =>
            listTableSummaries(tc, ns).map { summaries =>
              summaries.collect {
                case (ident, tableType)
                    if ident.namespace().nonEmpty &&
                      tp.matcher(quoteIfNeeded(ident.name())).matches() =>
                  (ident, toTableType(tableType))
              }.filter { case (_, typ) => tableTypes.exists(typ.equalsIgnoreCase) }
            }.getOrElse {
              tc.listTables(ns)
                .filter(i => tp.matcher(quoteIfNeeded(i.name())).matches())
                .map((_, TABLE))
                .toSeq
            }
          }.map { case (ident, typ) =>
            val schema = ident.namespace().map(quoteIfNeeded).mkString(".")
            val tableName = quoteIfNeeded(ident.name())
            Row(catalog.name(), schema, tableName, typ, "", null, null, null, null, null)
          }
        } else {
          val identifiers = namespaces.flatMap { ns =>
            tc.listTables(ns).filter(i => tp.matcher(quoteIfNeeded(i.name())).matches())
          }
          identifiers.map { ident =>
            // TODO: support view type once v2 ViewCatalog is mature enough.
            // load table is a time consuming operation
            val comment =
              tc.loadTable(ident).properties().getOrDefault(TableCatalog.PROP_COMMENT, "")
            val schema = ident.namespace().map(quoteIfNeeded).mkString(".")
            val tableName = quoteIfNeeded(ident.name())
            Row(catalog.name(), schema, tableName, TABLE, comment, null, null, null, null, null)
          }
        }

      case _ => Seq.empty[Row]
    }
  }

  private def getCustomSessionCatalogTablesOrViews(
      spark: SparkSession,
      tc: TableCatalog,
      namespaces: Array[Array[String]],
      tablePattern: String,
      tableTypes: Set[String],
      ignoreTableProperties: Boolean): Seq[Row] = {
    val tp = compilePattern(tc, tablePattern)
    val sessionCatalog = spark.sessionState.catalog

    def loadComment(ident: Identifier): String = {
      // loadTable may fail for v1-delegated tables, degrade REMARKS to "" on failure.
      attempt {
        tc.loadTable(ident).properties().getOrDefault(TableCatalog.PROP_COMMENT, "")
      } match {
        case Success(comment) => comment
        case Failure(e) =>
          warn(s"Failed to load table $ident for the comment", e)
          ""
      }
    }

    def listMatchingTables(ns: Array[String]): Seq[Identifier] = {
      // listTables returns local temp views (empty namespace), skip them.
      tc.listTables(ns).filter { t =>
        t.namespace().nonEmpty && tp.matcher(t.name()).matches()
      }.toSeq
    }

    val typedTables =
      if (ignoreTableProperties) {
        namespaces.flatMap { ns =>
          listTableSummaries(tc, ns).map { summaries =>
            summaries.collect {
              case (ident, tableType)
                  if ident.namespace().nonEmpty && tp.matcher(ident.name()).matches() =>
                (ident, toTableType(tableType), "")
            }.filter { case (_, typ, _) => tableTypes.exists(typ.equalsIgnoreCase) }
          }.getOrElse(listMatchingTables(ns).map((_, TABLE, "")))
        }
      } else {
        namespaces.flatMap { ns =>
          val tables = listMatchingTables(ns)
          if (tables.isEmpty) {
            Seq.empty
          } else {
            // Multi-level namespaces never refer to v1 tables, so only single-level
            // identifiers can join the batched metastore lookup below.
            val (v1Candidates, nonV1Tables) = tables.partition(_.namespace().length == 1)
            // One batched metastore call per namespace returns both the table type and the
            // comment for v1-backed tables, avoiding per-table loads.
            val batched = attempt {
              sessionCatalog.getTablesByName(
                v1Candidates.map(t => TableIdentifier(t.name(), Some(t.namespace().head))))
                .map(ct => (ct.database.toLowerCase, ct.identifier.table.toLowerCase) -> ct)
                .toMap
            }

            lazy val viewNames: Set[String] = ns.headOption match {
              case Some(db) if sessionCatalog.databaseExists(db) =>
                // listViews returns local temp views (empty namespace), skip them.
                sessionCatalog.listViews(db, tablePattern)
                  .filter(_.database.isDefined)
                  .map(_.table.toLowerCase)
                  .toSet
              case _ => Set.empty
            }

            // The v1 catalog is case-insensitive, so join case-insensitively to survive
            // catalogs whose identifiers differ in case from the metastore's.
            def joinKey(ident: Identifier): (String, String) =
              (ident.namespace().head.toLowerCase, ident.name().toLowerCase)

            val fromBatch = batched match {
              case Success(byName) =>
                v1Candidates.flatMap { ident =>
                  byName.get(joinKey(ident)).map { ct =>
                    val typ = if (ct.tableType == CatalogTableType.VIEW) VIEW else TABLE
                    (ident, typ, ct.comment.getOrElse(""))
                  }
                }
              case Failure(_) => Seq.empty
            }

            val unmatched = batched match {
              case Success(byName) =>
                v1Candidates.filterNot(ident => byName.contains(joinKey(ident)))
              case Failure(_) => v1Candidates
            }

            // Tables the batch does not cover: DSv2-only tables, or the whole namespace
            // when the batch fails to load.
            val remaining = nonV1Tables ++ unmatched
            fromBatch ++ remaining.map { ident =>
              val typ = if (viewNames.contains(ident.name().toLowerCase)) VIEW else TABLE
              (ident, typ, loadComment(ident))
            }
          }
        }
      }

    typedTables
      .filter { case (_, typ, _) =>
        ignoreTableProperties || tableTypes.exists(typ.equalsIgnoreCase)
      }
      .map { case (ident, typ, comment) =>
        val schema = ident.namespace().mkString(".")
        val tableName = ident.name()
        Row(tc.name(), schema, tableName, typ, comment, null, null, null, null, null)
      }
  }

  private def attempt[A](f: => A): Try[A] = {
    try {
      Success(f)
    } catch {
      // scala.util.Try does not catch LinkageError (e.g. a NoClassDefFoundError from a missing
      // storage handler dependency), but one unloadable table must not fail the operation.
      case NonFatal(e) => Failure(e)
      case e: LinkageError => Failure(e)
    }
  }

  // SPARK-52109 (4.1.0): added TableCatalog.listTableSummaries. Detect it by reflection
  // instead of by Spark version, since vendors may backport it. Use it only when the
  // catalog provides its own implementation, because the TableCatalog default calls
  // loadTable for every table, which is what this path avoids. Java reflection cannot
  // tell the two apart: scalac emits a bytecode forwarder to the interface default in
  // Scala classes that directly implement TableCatalog and rely on the default (e.g.
  // V2SessionCatalog), which looks like a real implementation by declaring class. Scala
  // reflection can, because forwarders never enter the Scala symbol table: resolving
  // the member lands on the TableCatalog symbol for classes relying on the default,
  // and on the implementing class for real implementations, including inherited ones.
  private[util] def implementsListTableSummaries(tc: TableCatalog): Boolean = {
    import scala.reflect.runtime.universe._
    attempt {
      val mirror = runtimeMirror(tc.getClass.getClassLoader)
      val tableCatalogSym = mirror.classSymbol(classOf[TableCatalog])
      val member = mirror.classSymbol(tc.getClass).toType.member(TermName("listTableSummaries"))
      member != NoSymbol && member.isMethod && member.owner != tableCatalogSym
    }.getOrElse(false)
  }

  // Lists (identifier, tableType) pairs via SPARK-52109 (4.1.0) listTableSummaries without
  // loading tables, or None when the catalog does not implement it or the call fails.
  private def listTableSummaries(
      tc: TableCatalog,
      ns: Array[String]): Option[Seq[(Identifier, String)]] = {
    if (implementsListTableSummaries(tc)) {
      attempt {
        invokeAs[Array[AnyRef]](tc, "listTableSummaries", (classOf[Array[String]], ns)).toSeq
          .map { summary =>
            invokeAs[Identifier](summary, "identifier") -> invokeAs[String](summary, "tableType")
          }
      }.toOption
    } else {
      None
    }
  }

  // Maps SPARK-52109 table types to the JDBC TABLE_TYPE; VIEW and METRIC_VIEW both surface as VIEW.
  private def toTableType(tableType: String): String = {
    if (tableType.equalsIgnoreCase(VIEW) || tableType.equalsIgnoreCase(METRIC_VIEW)) VIEW
    else TABLE
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
        val tp = compilePattern(catalog, tablePattern)
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
                toColumnResult(builtin.name(), t.database, t.identifier.table, f, i)
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
    // Temp views live in the session catalog regardless of the requested catalog, so label them
    // with SESSION_CATALOG to keep the rows resolvable, as getColumns does for temp view columns.
    views.map { ident =>
      Row(
        SESSION_CATALOG,
        ident.database.orNull,
        ident.table,
        VIEW,
        "",
        null,
        null,
        null,
        null,
        null)
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
