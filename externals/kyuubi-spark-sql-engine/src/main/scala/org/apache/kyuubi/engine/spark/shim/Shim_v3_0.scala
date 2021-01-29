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
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, SupportsNamespaces}
import org.apache.spark.sql.types.DataType

class Shim_v3_0 extends Shim_v2_4 {

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

  override def catalogExists(spark: SparkSession, catalog: String): Boolean = {
    spark.sessionState.catalogManager.isCatalogRegistered(catalog)
  }

  private def getSchemas(
      catalog: CatalogPlugin,
      schemaPattern: String): Seq[String] = catalog match {
    case catalog: CatalogExtension =>
      // DSv2 does not support pass schemaPattern transparently
      val schemas =
        (catalog.defaultNamespace()  ++ catalog.listNamespaces(Array()).map(_.head)).distinct
      schemas.filter(_.matches(schemaPattern))
    case catalog: SupportsNamespaces =>
      val rootSchema = catalog.listNamespaces()
      val allSchemas = listNamespaces(catalog, rootSchema).map(_.mkString("."))
      val schemas = (allSchemas ++: catalog.defaultNamespace().toSet)
      schemas.filter(_.matches(schemaPattern)).toSeq
  }

  private def listNamespaces(
      catalog: SupportsNamespaces, namespaces: Array[Array[String]]): Array[Array[String]] = {
    val children = namespaces.flatMap { ns =>
      catalog.listNamespaces(ns)
    }
    if (children.isEmpty) {
      namespaces.map(_.map(quoteIfNeeded))
    } else {
      namespaces.map(_.map(quoteIfNeeded)) ++: listNamespaces(catalog, children)
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

  override def getSchemas(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String): Seq[Row] = {
    val viewMgr = getGlobalTempViewManager(spark, schemaPattern)
    val manager = spark.sessionState.catalogManager
    if (catalogName == null) {
      val catalog = manager.currentCatalog
      (getSchemas(catalog, schemaPattern) ++ viewMgr).map(Row(_, catalog.name()))
    } else {
      val catalogPlugin = manager.catalog(catalogName)
      (getSchemas(catalogPlugin, schemaPattern) ++ viewMgr).map(Row(_, catalogName))
    }
  }

  override def toHiveString(value: Any, typ: DataType): String = {

    invokeScalaObject("org.apache.spark.sql.execution.HiveResult$",
      "toHiveString",
      (classOf[(AnyRef, DataType)], (value, typ)),
      (classOf[Boolean], Boolean.box(false))
    ).asInstanceOf[String]
  }
}
