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
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, SupportsNamespaces, TableCatalog}

import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim.SESSION_CATALOG

class CatalogShim_v3_0 extends CatalogShim_v2_4 {

  override def getCatalogs(spark: SparkSession): Seq[Row] = {

    // A [[CatalogManager]] is session unique
    val catalogMgr = spark.sessionState.catalogManager
    // get the custom v2 session catalog or default spark_catalog
    val sessionCatalog = invoke(catalogMgr, "v2SessionCatalog")
    val defaultCatalog = catalogMgr.currentCatalog

    val defaults = Seq(sessionCatalog, defaultCatalog).distinct
      .map(invoke(_, "name").asInstanceOf[String])
    val catalogs = getField(catalogMgr, "catalogs")
      .asInstanceOf[scala.collection.Map[String, _]]
    (catalogs.keys ++: defaults).distinct.map(Row(_))
  }

  private def getCatalog(spark: SparkSession, catalogName: String): CatalogPlugin = {
    val catalogManager = spark.sessionState.catalogManager
    if (catalogName == null || catalogName.isEmpty) {
      catalogManager.currentCatalog
    } else {
      catalogManager.catalog(catalogName)
    }
  }

  override def catalogExists(spark: SparkSession, catalog: String): Boolean = {
    spark.sessionState.catalogManager.isCatalogRegistered(catalog)
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

  /**
   * Forked from Apache Spark's org.apache.spark.sql.connector.catalog.CatalogV2Implicits
   */
  private def quoteIfNeeded(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }

  private def listNamespacesWithPattern(
      catalog: CatalogPlugin, schemaPattern: String): Array[Array[String]] = {
    val p = schemaPattern.r.pattern
    listAllNamespaces(catalog).filter { ns =>
      val quoted = ns.map(quoteIfNeeded).mkString(".")
      p.matcher(quoted).matches()
    }.distinct
  }

  private def getSchemasWithPattern(catalog: CatalogPlugin, schemaPattern: String): Seq[String] = {
    val p = schemaPattern.r.pattern
    listAllNamespaces(catalog).flatMap { ns =>
      val quoted = ns.map(quoteIfNeeded).mkString(".")
      if (p.matcher(quoted).matches()) {
        Some(quoted)
      } else {
        None
      }
    }.distinct
  }

  override def getSchemas(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String): Seq[Row] = {
    val viewMgr = getGlobalTempViewManager(spark, schemaPattern)
    val catalog = getCatalog(spark, catalogName)
    val schemas = getSchemasWithPattern(catalog, schemaPattern)
    (schemas ++ viewMgr).map(Row(_, catalog.name()))
  }

  override def getCatalogTablesOrViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      tableTypes: Set[String]): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)
    val namespaces = listNamespacesWithPattern(catalog, schemaPattern)
    catalog match {
      case builtin if builtin.name() == SESSION_CATALOG =>
        super.getCatalogTablesOrViews(
          spark, SESSION_CATALOG, schemaPattern, tablePattern, tableTypes)
      case tc: TableCatalog =>
        val tp = tablePattern.r.pattern
        val identifiers = namespaces.flatMap { ns =>
          tc.listTables(ns).filter(i => tp.matcher(quoteIfNeeded(i.name())).matches())
        }
        identifiers.map { ident =>
          val table = tc.loadTable(ident)
          // TODO: restore view type for session catalog
          val comment = table.properties().getOrDefault(TableCatalog.PROP_COMMENT, "")
          val schema = ident.namespace().map(quoteIfNeeded).mkString(".")
          val tableName = quoteIfNeeded(ident.name())
          Row(catalog.name(), schema, tableName, "TABLE", comment, null, null, null, null, null)
        }
      case _ => Seq.empty[Row]
    }
  }

  override protected def getColumnsByCatalog(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)

    catalog match {
      case builtin if builtin.name() == SESSION_CATALOG =>
        super.getColumnsByCatalog(
          spark, SESSION_CATALOG, schemaPattern, tablePattern, columnPattern)

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
    }
  }
}
